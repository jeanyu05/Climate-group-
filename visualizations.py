import sqlite3
import matplotlib
import matplotlib.pyplot as plt

def setup_climate_database(dbname):
    conn = sqlite3.connect(dbname)
    cur = conn.cursor()
    return cur, conn

def amd_trend_line(cur):

    cur.execute('''SELECT
                adj_close,
                date
                FROM amdstock
                ORDER BY date ASC
                ''')
    dates = []
    close = []
    for row in cur:
        dates.append(row[1])
        close.append(row[0])

    fig, ax = plt.subplots()
    ax.plot(dates, close)
    ax.set_xlabel('Dates')
    ax.set_ylabel('Closing Prices')
    ax.set_title('AMD Stock Trendline 12/12/2025 - 07/24/2025')
    

    plt.show()
def main():
    cur, conn = setup_climate_database('climate_data.db')
    amd_trend_line(cur)


main()