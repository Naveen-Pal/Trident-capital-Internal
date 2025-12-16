import re
import requests
import pandas as pd
from io import StringIO

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Referer": "https://www.screener.in/"
}

def search_company(company_name, cookies):
    url = "https://www.screener.in/api/company/search/"
    params = {"q": company_name, "v": 3, "fts": 1}

    r = requests.get(url, params=params, headers=HEADERS, cookies=cookies, timeout=15)
    r.raise_for_status()

    data = r.json()
    if not data:
        return None

    top = data[0]
    return top["id"], top["name"], top["url"]


def scrape_tables(company_url, cookies):
    url = f"https://www.screener.in{company_url}"
    r = requests.get(url, headers=HEADERS, cookies=cookies, timeout=15)
    r.raise_for_status()

    # Parse all tables
    tables = pd.read_html(StringIO(r.text))

    bs = None
    pnl = None

    # Identify tables by content
    for i, df in enumerate(tables):
        if df.empty: continue
        
        # Clean the first column to strict strings
        first_col_raw = df.iloc[:, 0].astype(str).tolist()
        first_col = [s.replace(u'\xa0', ' ').replace('+', '').strip() for s in first_col_raw]
        
        # Balance Sheet Keywords
        if ("Equity Capital" in first_col or "Equity Share Capital" in first_col) and \
           ("Borrowings" in first_col or "Total Liabilities" in first_col):
            bs = df
            continue

        # Profit & Loss Keywords
        if ("Sales" in first_col or "Revenue" in first_col) and \
           ("Operating Profit" in first_col):
            if pnl is None:
                pnl = df
            else:
                pnl = df

    if bs is None or pnl is None:
        raise ValueError(f"Could not find Balance Sheet or P&L tables on {url}")
    
    # Pre-process: Set index to first column
    bs.iloc[:, 0] = bs.iloc[:, 0].astype(str).str.replace(u'\xa0', ' ').str.replace('+', '').str.strip()
    pnl.iloc[:, 0] = pnl.iloc[:, 0].astype(str).str.replace(u'\xa0', ' ').str.replace('+', '').str.strip()
    
    bs.set_index(bs.columns[0], inplace=True)
    pnl.set_index(pnl.columns[0], inplace=True)
    
    # Remove duplicates
    pnl = pnl[~pnl.index.duplicated(keep='first')]

    # Convert to numeric
    for col in bs.columns:
        if bs[col].dtype == object:
             bs[col] = bs[col].astype(str).str.replace(',', '', regex=False)
        bs[col] = pd.to_numeric(bs[col], errors='coerce')
        
    for col in pnl.columns:
        if pnl[col].dtype == object:
             pnl[col] = pnl[col].astype(str).str.replace(',', '', regex=False)
             pnl[col] = pnl[col].astype(str).str.replace('%', '', regex=False)
        pnl[col] = pd.to_numeric(pnl[col], errors='coerce')

    return bs, pnl


def calculate_ratios(bs, pnl, company):
    results = []

    for col in bs.columns:
        # Skip garbage columns
        if pd.isna(col) or str(col).strip() == "":
            continue

        try:
            debt = bs.at["Borrowings", col]
            
            equity_key = "Equity Capital" if "Equity Capital" in bs.index else "Equity Share Capital"
            if equity_key not in bs.index and "Share Capital" in bs.index:
                equity_key = "Share Capital"
            
            equity = bs.at[equity_key, col] + bs.at["Reserves", col]

            # P&L keys
            sales_key = "Sales" if "Sales" in pnl.index else "Revenue"
            revenue = pnl.at[sales_key, col]
            
            op_profit = pnl.at["Operating Profit", col]

            # OPM
            opm_key = "OPM %" if "OPM %" in pnl.index else "OPM"
            
            opm = None
            if opm_key in pnl.index:
               opm = pnl.at[opm_key, col]
            
            if pd.isna(opm):
                opm = op_profit / revenue if revenue else None
            else:
                if isinstance(opm, (int, float, str)):
                     try:
                        val = float(str(opm).replace('%', ''))
                        if val > 1: val = val / 100.0
                        opm = val
                     except:
                        pass

            # ROCE
            # Approx Capital Employed = Equity + Debt.
            # ROCE = EBIT / (Equity + Debt).
            # EBIT = Net Profit + Tax + Interest (since PnL has Interest, Tax, Net Profit).
            
            pbt_key = "Profit before tax" if "Profit before tax" in pnl.index else None
            int_key = "Interest" if "Interest" in pnl.index else None
            
            ebit = None
            if pbt_key and int_key:
                ebit = pnl.at[pbt_key, col] + pnl.at[int_key, col]
            
            roce = None
            if ebit is not None and (equity + debt) > 0:
                 roce = ebit / (equity + debt)

            results.append({
                "Company": company,
                "Month": col,
                "Debt_to_Equity": clean_val(debt / equity if equity else None),
                "Operating_Profit_Margin": clean_val(opm),
                "ROCE": clean_val(roce)
            })
        except KeyError:
            continue

    return results

def clean_val(val):
    if val is None or pd.isna(val):
        return None
    return float(val)
