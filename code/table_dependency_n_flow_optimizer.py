# Databricks notebook source
# Pre-requisite libraries for the notebook
!pip install networkx --quiet
!pip install plotly --quiet
!pip install ipysigma --quiet

# COMMAND ----------

# Class definition with methods to extract upstream dependencies for the target notebook
import re
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
import base64

class PySparkDependencyTracker:
    """
    This is a class defined to encapsulate the methods used to parse the notebook code and evaluate the following:
    - External dependency of sql object on the target notebook
    - Used variable full paths evaluted by recusrive substitution 
    """

    def __init__(self, path):
        self.path = path
        self.notebook_name = os.path.basename(path)
        self.notebook_table = self.__get_notebook_table_name(self.path, self.notebook_name)
        self.codebase = self.__get_notebook_code_base(path)

    def __get_notebook_table_name(self, path, table_name):
        mapd = {'tfm_slv_network_assets': 'lhdev.tfm_slv_network_assets.' , 'slv_network_assets' : 'lhdev.slv_network_assets.'}
        for k in mapd:
            if k in path:
                return mapd[k] + table_name

    def __get_notebook_code_base(self, path):
        myToken = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
        databricksURL = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
        w = WorkspaceClient(host=databricksURL, token=myToken)
        export_response = w.workspace.export(path, format=workspace.ExportFormat.SOURCE)
        code_data = base64.b64decode(export_response.content).decode('utf-8')
        return code_data

    def __get_notebook_declared_params(self): # widgets/spark.conf
        # Obtain the notebook paramters 
        notebook_params = re.findall(r'dbutils\.widgets\.text\("([^"]+)",\s*"([^"]+)"\)',self.codebase, re.IGNORECASE)
        conf_params = re.findall(r'spark\.conf\.set\("([^"]*var[^"]*)",\s*f\'([^\']*)\'',self.codebase, re.IGNORECASE)
        notebook_params = notebook_params + conf_params
        self.notebookparams = notebook_params # Global
        return notebook_params

    def __get_declared_variable_value(self, varname):
        for p in self.notebookparams:
            if p[0] == varname:
                return p[0],p[1]
            
        matches = re.findall(r'(\w+)\s*=\s*([^;\n]+)',self.codebase, re.IGNORECASE | re.MULTILINE)
        for k,v in matches:
            if k == varname:
                value = re.sub(r"f'","'", v, count=1) # Remove the substitute char f from begining of the string
                value = re.sub(r"^[\"']|[\"']$", "", value) # Cleanup the start/end quotes Double/single
                if not any(p for p in self.notebookparams if p[0] == 'path'):
                    self.notebookparams.append((k,value))
                return k,value
        return None, None

    def __substitute_variables(self, data, key, visited=None):
        if visited is None:
            visited = set()
        
        if key in visited:
            raise ValueError(f"Circular reference detected for key: {key}")
        
        visited.add(key)
        
        value = data[key]
        pattern = r'\{([^}]*)\}' # r'\$(\w+)'
        
        def replace_match(match):
            var_name = match.group(1)
            if var_name in data:
                return self.__substitute_variables(data, var_name, visited)
            else:
                return match.group(0)  # Return the original match if the variable is not found
        
        while re.search(pattern, value):
            value = re.sub(pattern, replace_match, value)
        
        data[key] = value
        visited.remove(key)
        return value

    def __substitute_all_variables(self, data):
        for key in data:
            data[key] = self.__substitute_variables(data, key)
        return data

    def __build_nested_variable_substitution_hierarchy(self):
        for p in self.notebookparams:
            matches = re.findall(r'\{([^}]*)\}', p[1], re.IGNORECASE)
            for m in matches:
                k, v = self.__get_declared_variable_value(m)

        kv_dict = {}
        for p in self.notebookparams:
            kv_dict[p[0]] = p[1]

        return kv_dict

    def get_notebook_variables(self):
        notebook_declared_params = self.__get_notebook_declared_params()
        key_pair = self.__build_nested_variable_substitution_hierarchy()
        self.notebookvariables =  self.__substitute_all_variables(key_pair)
        return self.notebookvariables

    def get_upstream_objects(self):
        _ = self.get_notebook_variables()

        usage_patterns = [
            r'FROM\s+([^\s;]+)',
            r'JOIN\s+([^\s;]+)',
            r'spark\.table\s*\(\s*f?[\'"]([^\'"]+)[\'"]\s*\)'
        ]

        blocks = re.split(r'# COMMAND ----------|\n--', self.codebase)
        dependent_tables = []
        for block in blocks:
            for pattern in usage_patterns:
                found_tables = re.findall(pattern, block, re.IGNORECASE | re.MULTILINE)
                if len(found_tables) > 0:
                    dependent_tables = dependent_tables + list(set(found_tables))

        for k in self.notebookvariables:
            dependent_tables = [tbl.replace('{' + k + '}', self.notebookvariables[k]) for tbl in dependent_tables]
        
        for k in self.notebookvariables:
            dependent_tables = [tbl.replace('${var.' + k + '}', self.notebookvariables[k]) for tbl in dependent_tables]
        
        sql = f"""
        Select concat_ws('.', table_catalog , table_schema, table_name) as valid_tables_upstream
        From system.information_schema.tables
        where concat_ws('.', table_catalog , table_schema, table_name) IN ({','.join(f"'{x}'" for x in dependent_tables)})
        order by table_catalog, table_schema, table_name
        """
        result = spark.sql(sql)

        valid_tables = []
        for r in result.collect():
            valid_tables.append(r.valid_tables_upstream)

        custom_source = ['parquet.', 'delta.']
        for itm in dependent_tables:
            if any(x in itm for x in custom_source):
                valid_tables.append(itm)

        return valid_tables


# COMMAND ----------

import os
import pandas as pd

notebook_list = [
    '/Workspace/Users/username@org.com.au/silver/silver_tbl',
    '/Workspace/Users/username@org.com.au/transform/collated_data_src_a',
    '/Workspace/Users/username@org.com.au/transform/collated_data_src_b'
]

dependency_dataframe = None
for pth in notebook_list:
    code_tracer = PySparkDependencyTracker(pth)
    upstream_obj = code_tracer.get_upstream_objects()
    print(code_tracer.notebook_name, code_tracer.notebookvariables)
    if len(upstream_obj) > 0:
        temp_df = pd.DataFrame(data={'upstream_objects' : upstream_obj})
        temp_df['notebook_name'] = code_tracer.notebook_table
        if dependency_dataframe is None:
            dependency_dataframe = temp_df
        else:
            dependency_dataframe = pd.concat([dependency_dataframe , temp_df], axis = 0)

    del code_tracer 
display(dependency_dataframe)

# COMMAND ----------

dependency_dataframe = dependency_dataframe[dependency_dataframe['upstream_objects'] != dependency_dataframe['notebook_name']] # Remove circular dependency

# COMMAND ----------

import networkx as nx
G = nx.DiGraph()

for n in dependency_dataframe['notebook_name'].unique():
    G.add_node(n)

for idx, row  in dependency_dataframe.iterrows():
    G.add_edge(row['upstream_objects'] , row['notebook_name'])

print(f'The graph has node count of {len(G.nodes())} and with edges {len(G.edges())}')

# COMMAND ----------

#https://networkx.org/documentation/stable/reference/algorithms/generated/networkx.algorithms.dag.transitive_reduction.html
tr = nx.transitive_reduction(G)

# COMMAND ----------

import networkx as nx
from ipysigma import Sigma

# Importing a gexf graph
# g = nx.read_gexf('/Workspace/Users/amol.pandey@essentialenergy.com.au/sample/optimized_graph.gexf')

# Displaying the graph with a size mapped on degree and
# a color mapped on a categorical attribute of the nodes
Sigma(tr, node_size=tr.degree, node_color='category')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Experimental - Ignore below

# COMMAND ----------

# Experimental
# https://networkx.org/documentation/stable/reference/drawing.html
# nx.draw(G,pos=nx.kamada_kawai_layout(G), with_labels=True, font_size=8)
# nx.draw(tr,pos=nx.kamada_kawai_layout(tr),with_labels=True,font_size=8)

# COMMAND ----------

# Experimental
# https://plotly.com/python/network-graphs/
import plotly.graph_objects as go

layout = nx.kamada_kawai_layout(tr)

edge_x = []
edge_y = []
for edge in G.edges():
    x0, y0 = layout[edge[0]][0] , layout[edge[0]][1]
    x1, y1 = layout[edge[1]][0] , layout[edge[1]][1]
    edge_x.append(x0)
    edge_x.append(x1)
    edge_x.append(None)
    edge_y.append(y0)
    edge_y.append(y1)
    edge_y.append(None)

edge_trace = go.Scatter(
    x=edge_x, y=edge_y,
    line=dict(width=0.5, color='#888'),
    hoverinfo='none',
    mode='lines')

node_x = []
node_y = []
for node in G.nodes():
    x, y = layout[node][0] , layout[node][1]
    node_x.append(x)
    node_y.append(y)

node_trace = go.Scatter(
    x=node_x, y=node_y,
    mode='markers',
    hoverinfo='text',
    marker=dict(
        showscale=True,
        # colorscale options
        #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
        #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
        #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
        colorscale='YlGnBu',
        reversescale=True,
        color=[],
        size=10,
        colorbar=dict(
            thickness=15,
            title=dict(
              text='Node Connections',
              side='right'
            ),
            xanchor='left',
        ),
        line_width=2))

# COMMAND ----------

# Experimental
node_adjacencies = []
node_text = []
for node, adjacencies in enumerate(G.adjacency()):
    node_adjacencies.append(len(adjacencies[1]))
    node_text.append(f'{adjacencies[0]} # of connections: '+str(len(adjacencies[1])))

node_trace.marker.color = node_adjacencies
node_trace.text = node_text

# COMMAND ----------

# Experimental
fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                title=dict(
                    text="Optimized worlflow for notebooks",
                    font=dict(
                        size=16
                    )
                ),
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                # annotations=[ dict(
                #     text="Python code: <a href='https://plotly.com/python/network-graphs/'> https://plotly.com/python/network-graphs/</a>",
                #     showarrow=False,
                #     xref="paper", yref="paper",
                #     x=0.005, y=-0.002 ) ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                )
fig.show()

# COMMAND ----------

# Experimental
nx.write_gexf(G, 'original_graph.gexf') # Export for Gelphi
nx.write_gexf(tr, 'optimized_graph.gexf') # Export for Gelphi