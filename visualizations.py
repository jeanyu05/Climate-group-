import sqlite3
from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt

DB = 'climate_data.db'

def plot_eia_avg_consumption(db_path: str = DB,
                             start_year: int = 2013,
                             end_year: int = 2023,
                             top_n: int = 10,
                             save_path: Optional[str] = None) -> Optional[str]:
    """
    Load EIA data from the SQLite DB, compute average consumption per state
    between start_year and end_year, and plot a bar chart of the top_n states.
    If save_path is provided the plot is saved and the path returned, otherwise the plot is shown.
    """
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query("SELECT state, year, value FROM eia_consumption", conn)
    conn.close()

    if df.empty:
        raise ValueError("No EIA data found in database.")

    mask = (df['year'] >= start_year) & (df['year'] <= end_year)
    sub = df[mask].copy()
    if sub.empty:
        raise ValueError(f"No EIA rows in range {start_year}-{end_year}.")

    agg = sub.groupby('state')['value'].mean().sort_values(ascending=False).head(top_n)

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(agg.index, agg.values, color='C2')
    ax.set_ylabel('Average Consumption')
    ax.set_title(f'Average Energy Consumption per State ({start_year}-{end_year})')
    plt.xticks(rotation=45, ha='right')
    for b in bars[:5]:
        b.set_edgecolor('black')
        b.set_linewidth(1.0)
    plt.tight_layout()

    if save_path:
        fig.savefig(save_path, dpi=150)
        plt.close(fig)
        return save_path
    else:
        plt.show()
        return None

if __name__ == '__main__':
    plot_eia_avg_consumption()