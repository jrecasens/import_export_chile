import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def aggregate_canola_imports(input_df, country_name='TODOS'):
    input_df = input_df.fillna(0)
    # Aggregate all countries and compute weighted average
    g = input_df.groupby(['fecha_month'], as_index=False)
    df = g.apply(lambda x: pd.Series([np.sum(x['cantidad_quintal']),
                                                          np.average(x['precio_fob_usd_quintal'], weights=x['cantidad_quintal']),
                                                          np.average(x['precio_fob_cad_quintal'], weights=x['cantidad_quintal']),
                                                          np.average(x['precio_cif_usd_quintal'], weights=x['cantidad_quintal']),
                                                          np.average(x['precio_cif_cad_quintal'], weights=x['cantidad_quintal'])
                                                          ]))
    df.rename(columns={0: 'cantidad_quintal',
                                           1: 'precio_fob_usd_quintal',
                                           2: 'precio_fob_cad_quintal',
                                           3: 'precio_cif_usd_quintal',
                                           4: 'precio_cif_cad_quintal'},
                                  inplace=True)
    df['pais_nombre_origen'] = country_name
    df = df[['fecha_month', 'pais_nombre_origen',
                                                     'cantidad_quintal' ,'precio_fob_usd_quintal',
                                                     'precio_fob_cad_quintal' ,'precio_cif_usd_quintal',
                                                     'precio_cif_cad_quintal']]
    return df

def generate_missing_dates(df, date_from, date_to):

    unique_countries = df['pais_nombre_origen'].unique()

    for c in unique_countries:
        missing_dates_df = pd.DataFrame({
            'fecha_month': pd.date_range(date_from, date_to, freq='MS'),
            'pais_nombre_origen': c,
            'cantidad_quintal': float('nan'),
            'precio_fob_usd_quintal': float('nan'),
            'precio_fob_cad_quintal': float('nan'),
            'precio_cif_usd_quintal': float('nan'),
            'precio_cif_cad_quintal': float('nan')},
            columns=["fecha_month",
                     "pais_nombre_origen",
                     "cantidad_quintal",
                     "precio_fob_usd_quintal",
                     "precio_fob_cad_quintal",
                     "precio_cif_usd_quintal",
                     "precio_cif_cad_quintal"]
        )
        a_list = df['pais_nombre_origen'] == c
        months_to_delete = df[a_list].fecha_month.unique()
        missing_lst = ~missing_dates_df['fecha_month'].isin(months_to_delete)
        missing_dates_df = missing_dates_df[missing_lst]

        missing_dates_df = missing_dates_df.astype(df.dtypes.to_dict())


        df = pd.concat([df, missing_dates_df], ignore_index=True, sort=False)

    df['fecha_month'] = pd.to_datetime(
        df['fecha_month'],
        format="%Y-%m-%d",
    )

    df.cantidad_quintal.fillna(0, inplace=True)

    del months_to_delete

    return df


def create_imports_canola_plot(df,
                               title="Pais: Todos"):
    # Create figure with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Add Bar Chart for quantity
    fig.add_trace(go.Bar(
        x=df['fecha_month'],
        y=df['cantidad_quintal'],
        name="Cantidad",
        text=df['cantidad_quintal'].apply(lambda x: "{:,.0f}".format(x)),
        showlegend=True
    ),
        secondary_y=False
    )

    # Add traces for FOB price
    fig.add_trace(go.Scatter(x=df['fecha_month'],
                             y=df['precio_fob_usd_quintal'],
                             name="Precio FOB",
                             text=df['precio_fob_usd_quintal'].apply(lambda x: "${:.0f}".format(x)),
                             textposition="bottom right",
                             connectgaps=True,
                             mode="lines+markers+text",
                             hovertemplate='.',
                             showlegend=True
                             ),
                  secondary_y=True
                  )

    # Add traces for CIF price
    fig.add_trace(go.Scatter(x=df['fecha_month'],
                             y=df['precio_cif_usd_quintal'],
                             name="Precio CIF",
                             text=df['precio_cif_usd_quintal'].apply(lambda x: "${:.0f}".format(x)),
                             textposition="top right",
                             connectgaps=True,
                             mode="lines+markers+text",
                             hovertemplate='.',
                             showlegend=True
                             ),
                  secondary_y=True)

    fig.update_layout(title_text=title, hovermode="x unified")

    fig.update_traces(hovertemplate='%{y:,.1f}')

    # Set x-axis title
    fig.update_xaxes(title_text="Mes")
    # Set y-axes titles
    fig.update_yaxes(title_text="Cantidad [100 kg]", secondary_y=False)
    fig.update_yaxes(title_text="Precio [USD / 100 kg]", secondary_y=True)

    fig.update_layout(
        uniformtext_minsize=12,
        xaxis=dict(
            showline=True,
            showgrid=False,
            showticklabels=True,
            linecolor='rgb(204, 204, 204)',
            linewidth=2,
            ticks='outside',
            tickfont=dict(
                family='Arial',
                size=12,
                color='rgb(82, 82, 82)',
            ),
        ),
        yaxis=dict(
            showgrid=False,
            zeroline=False,
            showline=False,
            showticklabels=True,
        ),
        autosize=False,
        width=700,
        height=400,
        margin=dict(
            autoexpand=False,
            l=60,
            r=15,
            t=45,
        ),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        plot_bgcolor='white'
    )

    return fig