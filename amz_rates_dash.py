
import datetime as dt
from dash import Dash, Input, Output, dcc, html
import dash_bootstrap_components as dbc
import getpass
import pandas as pd
import plotly
import plotly.express as px
import psycopg2
from sqlalchemy import create_engine, text


redshift_user = 'tbruks'
redshift_pass = 'UZoh>choreeb8aegae4R'
redshift_host = "central.c30hiwrajgjj.us-east-1.redshift.amazonaws.com"
redshift_port = 5439
dbname = 'analytics'

engine_string = "postgresql+psycopg2://%s:%s@%s:%d/%s" % (
    redshift_user, redshift_pass, redshift_host, redshift_port, dbname)
engine1 = create_engine(engine_string)

cols = {
    'Age': 'demo_age_bucket_4way',
    'Race': 'demo_combined_ethnicity_wb_samp',
    'Party': 'demo_combined_party',
    'Core/Expansion': 'demo_core_expansion',
    'Income': 'demo_income_bucket_full',
    'Region': 'demo_natl_region',
    'NYT Urbanicity': 'demo_nyt_urbanicity_topmm_samp',
    'RCT Flag': 'demo_rct_flag',
    'Sex:Female': 'demo_sex_female'
}

df = []

for var, level in cols.items():
    base_query = f"""

            SELECT '00 Topline' as var
                   , '00 Topline' as level
                   , rr.date_called
                   , COUNT(DISTINCT CASE WHEN rr.phone_type = 'L' AND rr.vendor != 'IVR' THEN phone ELSE NULL END) AS n_called_landline
                   , COUNT(DISTINCT CASE WHEN rr.phone_type = 'C' THEN phone ELSE NULL END) AS n_called_cell
                   , COUNT(DISTINCT CASE WHEN rr.phone_type = 'L' AND rr.vendor = 'IVR' THEN phone ELSE NULL END) AS n_called_ivr
                   , SUM(CASE WHEN ret.phone_type = 'L' AND ret.vendor != 'IVR' THEN age_match ELSE 0 END) AS n_matched_landline
                   , SUM(CASE WHEN ret.phone_type = 'C' THEN age_match ELSE 0 END) AS n_matched_cell
                   , SUM(CASE WHEN ret.phone_type = 'L' AND ret.vendor = 'IVR' THEN age_match ELSE 0 END) AS n_matched_ivr
                   , SUM(CASE WHEN rr.disp = 1 then 1 else 0 end) as total_completes
             FROM ext_bl_survey.amazon_infl_experiment_20220418_returns_raw rr
             LEFT JOIN ext_bl_survey.amazon_infl_experiment_20220418_returns ret ON rr.phone_called_id = ret.phone_called_id
             LEFT JOIN ext_bl_survey.amazon_infl_experiment_20220418_universe univ ON rr.voterbase_id = univ.voterbase_id
             WHERE rr.vendor = 'CMG'
             AND rr.date_called <> '2022-01-28'
            GROUP BY 1, 2, 3

    UNION ALL

            SELECT '{var}' as var
                   , COALESCE(ret.{level}, univ.{level}) as level
                   , rr.date_called
                   , COUNT(DISTINCT CASE WHEN rr.phone_type = 'L' AND rr.vendor != 'IVR' THEN phone ELSE NULL END) AS n_called_landline
                   , COUNT(DISTINCT CASE WHEN rr.phone_type = 'C' THEN phone ELSE NULL END) AS n_called_cell
                   , COUNT(DISTINCT CASE WHEN rr.phone_type = 'L' AND rr.vendor = 'IVR' THEN phone ELSE NULL END) AS n_called_ivr
                   , SUM(CASE WHEN ret.phone_type = 'L' AND ret.vendor != 'IVR' THEN age_match ELSE 0 END) AS n_matched_landline
                   , SUM(CASE WHEN ret.phone_type = 'C' THEN age_match ELSE 0 END) AS n_matched_cell
                   , SUM(CASE WHEN ret.phone_type = 'L' AND ret.vendor = 'IVR' THEN age_match ELSE 0 END) AS n_matched_ivr
                   , SUM(CASE WHEN rr.disp = 1 then 1 else 0 end) as total_completes
             FROM ext_bl_survey.amazon_infl_experiment_20220418_returns_raw rr
             LEFT JOIN ext_bl_survey.amazon_infl_experiment_20220418_returns ret ON rr.phone_called_id = ret.phone_called_id
             LEFT JOIN ext_bl_survey.amazon_infl_experiment_20220418_universe univ ON rr.voterbase_id = univ.voterbase_id
             WHERE rr.vendor = 'CMG'
             AND rr.date_called <> '2022-01-28'
            GROUP BY 1, 2, 3

    ;
    """
    df_temp = pd.read_sql_query(base_query, engine1, parse_dates=[
                                'date_called'])  # this might be slow or inefficient. Is it making a df each time?
    df.append(df_temp)

metrics_df = pd.concat(df)

metrics_df['total_calls'] = metrics_df[[
    'n_called_landline', 'n_called_cell', 'n_called_ivr']].sum(axis=1)
metrics_df['total_matches'] = metrics_df[[
    'n_matched_landline', 'n_matched_cell', 'n_matched_ivr']].sum(axis=1)
metrics_df['total_response_rate'] = metrics_df['total_completes'] / \
    metrics_df['total_calls']  # response rate = completes/attempts
metrics_df['total_match_rate'] = metrics_df['total_matches'] / \
    metrics_df['total_completes']  # match_rate = matches/completes

# create a response rate plot for each variable's level
response_fig = []
for i, var in enumerate(metrics_df['var'].unique(), start=1):
    temp_df = metrics_df[(metrics_df['var'] == var) & (
        metrics_df['total_calls'] > 100)].sort_values(by=['date_called'])
    fig = px.line(temp_df, x='date_called', y='total_response_rate',
                  color='level', title=var + " Response Rate")

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(step="all"),
                dict(count=1, label="yesterday",
                     step="day", stepmode="backward"),
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward")
            ])
        )
    )
    # response_fig.append(dcc.Graph(figure=fig))
    response_fig.append(dcc.Graph(figure=fig))

# create a match rate plot for each variable's level
match_fig = []
for i, var in enumerate(metrics_df['var'].unique(), start=1):
    temp_df = metrics_df[(metrics_df['var'] == var) & (
        metrics_df['total_calls'] > 100)].sort_values(by=['date_called'])
    fig = px.line(temp_df, x='date_called', y='total_match_rate',
                  color='level', title=var + " Match Rate")

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list([
                dict(step="all"),
                dict(count=1, label="yesterday",
                     step="day", stepmode="backward"),
                dict(count=1, label="1m", step="month", stepmode="backward"),
                dict(count=6, label="6m", step="month", stepmode="backward")
            ])
        )
    )
    match_fig.append(dcc.Graph(figure=fig))


#This CSS is not working
external_stylesheets = [
    {
        "href": "https://fonts.googleapis.com/css?family=Lato:wght@400;700&display=swap",
        "rel": "stylesheet",
    },
]
app = Dash(__name__, external_stylesheets=external_stylesheets)
app.title = "Amazon Dashboard"

app.layout = html.Div([
    html.H2("Response Rate By Stratification Variable"),
    html.Div(children=response_fig),
    html.Br(),
    html.H2("Match Rate By Stratification Variable"),
    html.Div(children=match_fig)
])

if __name__ == '__main__':
    app.run_server(debug=True)


