import os
import io
import csv
import pandas as pd
from flask import Flask, render_template, request, jsonify, send_file
from dotenv import load_dotenv
import logic

# Load environment variables
load_dotenv()

app = Flask(__name__)

def get_cookies():
    session_id = os.getenv("SESSION_ID")
    csrf_token = os.getenv("CSRF_TOKEN")
    if not session_id or not csrf_token:
        raise ValueError("Cookies missing in .env")
    
    return {
        "sessionid": session_id,
        "csrftoken": csrf_token
    }

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/analyze", methods=["POST"])
def analyze():
    try:
        data = request.json
        companies = data.get("companies", [])
        companies = [c.strip() for c in companies if c.strip()]
        
        if not companies:
            return jsonify({"error": "No companies provided"}), 400

        cookies = get_cookies()
        all_results = []
        errors = []

        for company in companies:
            try:
                search = logic.search_company(company, cookies)
                if not search:
                    errors.append(f"{company}: Not found")
                    continue
                
                company_id, proper_name, company_url = search
                
                # Scrape and calculate
                bs, pnl = logic.scrape_tables(company_url, cookies)
                ratios = logic.calculate_ratios(bs, pnl, proper_name)
                
                all_results.extend(ratios)
                
            except Exception as e:
                errors.append(f"{company}: {str(e)}")

        return jsonify({
            "results": all_results,
            "errors": errors
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/download", methods=["POST"])
def download():
    try:
        data = request.json
        results = data.get("results", [])
        
        if not results:
            return jsonify({"error": "No data to download"}), 400
            
        # Create DataFrame
        df = pd.DataFrame(results)
        
        # Pivot: 
        # Index: Company
        # Columns: Month
        # Values: Debt_to_Equity, Operating_Profit_Margin, ROCE
        df_pivot = df.pivot(index="Company", columns="Month", values=["Debt_to_Equity", "Operating_Profit_Margin", "ROCE"])
        
        # Swap levels so Month is on top: (Month, Metric)
        df_pivot.columns = df_pivot.columns.swaplevel(0, 1)
        
        # Sort columns by Date
        # Helper to parse "Mar 2014" -> datetime
        def parse_month(m):
            try:
                return pd.to_datetime(m, format="%b %Y")
            except:
                return pd.Timestamp.max # Push errors to end

        # Get unique months and sort them
        unique_months = sorted(list(set(df_pivot.columns.get_level_values(0))), key=parse_month)
        
        # Reindex axis 1 with sorted months
        # We want order: Month1-MetricA, Month1-MetricB ...
        # So we sort the MultiIndex by Month (primary) and Metric (secondary)
        # But sorting text "Mar 2014" alphabetically is wrong.
        # So we manually construct the desired column order.
        
        # Reindex axis 1 with sorted months
        metrics = ["Debt_to_Equity", "Operating_Profit_Margin", "ROCE"]
        new_columns = []
        for m in unique_months:
            for metric in metrics:
                if (m, metric) in df_pivot.columns:
                    new_columns.append((m, metric))
                    
        df_pivot = df_pivot.reindex(columns=new_columns)

        # Construct CSV manually for "merged" header effect
        # Row 1: Company, Month1, , , Month2, , , ...
        # Row 2: , Metric1, Metric2, Metric3, Metric1, ...
        
        # 1. Header Row 1
        header_row_1 = ["Company"]
        header_row_2 = [""] # Empty under Company
        
        current_month = None
        for col in df_pivot.columns:
            month = col[0] # MultiIndex level 0
            metric = col[1] # MultiIndex level 1
            
            # Row 2 always has metric name
            header_row_2.append(metric)
            
            # Row 1 has month only if it changes
            if month != current_month:
                header_row_1.append(month)
                current_month = month
            else:
                header_row_1.append("") # Empty for merged look
        
        # Write to string buffer
        si = io.StringIO()
        writer = csv.writer(si)
        writer.writerow(header_row_1)
        writer.writerow(header_row_2)
        
        # Write data rows
        # Index is still Company (index) + MultiIndex columns
        # Reset index to access Company easily? 
        # Actually easiest is to iterate.
        
        for index, row in df_pivot.iterrows():
            # index is Company Name
            csv_row = [index]
            csv_row.extend(row.tolist())
            writer.writerow(csv_row)
        
        output = io.BytesIO()
        output.write(si.getvalue().encode('utf-8'))
        output.seek(0)
        
        return send_file(
            output,
            mimetype="text/csv",
            as_attachment=True,
            download_name="screener_ratios_pivoted.csv"
        )
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)
