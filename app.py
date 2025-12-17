import os
import io
import csv
import pandas as pd
import xlsxwriter
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
            
        # Create Excel file in memory
        output = io.BytesIO()
        workbook = xlsxwriter.Workbook(output, {'in_memory': True})
        worksheet = workbook.add_worksheet("Ratios")

        # Formats
        header_fmt = workbook.add_format({'bold': True, 'bg_color': '#DDEBF7', 'border': 1})
        num_fmt = workbook.add_format({'num_format': '0.00'})
        pct_fmt = workbook.add_format({'num_format': '0.00%'})
        
        # Headers
        headers = ["Company", "Month", "Debt/Equity", "OPM %", "ROCE %"]
        raw_headers = ["Borrowings", "Equity Capital", "Reserves", "Sales", "Op Profit", "PBT", "Interest"]
        
        # Write Main Headers (A-E)
        for col, h in enumerate(headers):
            worksheet.write(0, col, h, header_fmt)

        # Write Raw Headers (starting at K -> col 10)
        RAW_START_COL = 10 
        for col, h in enumerate(raw_headers):
            worksheet.write(0, RAW_START_COL + col, "Raw " + h, header_fmt)

        # Prepare data for merging
        # Assumed results are already sorted/grouped by company from logic.py
        # But let's be safe and group them if needed or just iterate.
        
        # Merge Format
        merge_fmt = workbook.add_format({
            'bold': True,
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        
        # We need to write data row by row, but merge Company column.
        # Let's write all data first, then apply merges for Company column.
        # Or track ranges.
        
        current_company = None
        start_row = 1
        
        for row_idx, row_data in enumerate(results, start=1):
            company = row_data.get("Company")
            
            # Write Month and Data (Cols B onwards)
            worksheet.write(row_idx, 1, row_data.get("Month"))
            
            # Raw Data
            raw_keys = [
                "Raw_Borrowings", "Raw_Equity_Share_Capital", "Raw_Reserves", 
                "Raw_Sales", "Raw_Operating_Profit", "Raw_Profit_before_tax", "Raw_Interest"
            ]
            
            for i, key in enumerate(raw_keys):
                val = row_data.get(key, 0)
                if val is None: val = 0
                worksheet.write_number(row_idx, RAW_START_COL + i, float(val), num_fmt)

            # Formulas
            excel_row = row_idx + 1
            f_de = f'=IF((L{excel_row}+M{excel_row})<>0, K{excel_row}/(L{excel_row}+M{excel_row}), 0)'
            worksheet.write_formula(row_idx, 2, f_de, num_fmt)

            f_opm = f'=IF(N{excel_row}<>0, O{excel_row}/N{excel_row}, 0)'
            worksheet.write_formula(row_idx, 3, f_opm, pct_fmt)

            f_roce = f'=IF((L{excel_row}+M{excel_row}+K{excel_row})<>0, (P{excel_row}+Q{excel_row})/(L{excel_row}+M{excel_row}+K{excel_row}), 0)'
            worksheet.write_formula(row_idx, 4, f_roce, pct_fmt)
            
            # Handle Merging Logic
            if company != current_company:
                # If we have a previous company to merge
                if current_company is not None:
                    end_row = row_idx - 1
                    if start_row == end_row:
                        worksheet.write(start_row, 0, current_company, merge_fmt)
                    else:
                        worksheet.merge_range(start_row, 0, end_row, 0, current_company, merge_fmt)
                
                # Start new block
                current_company = company
                start_row = row_idx

        # Merge the last company/block
        if current_company is not None:
            end_row = len(results)
            if start_row == end_row:
                worksheet.write(start_row, 0, current_company, merge_fmt)
            else:
                worksheet.merge_range(start_row, 0, end_row, 0, current_company, merge_fmt)

        # Adjust widths
        worksheet.set_column(0, 0, 25) # Company
        worksheet.set_column(1, 1, 12) # Month
        worksheet.set_column(2, 4, 12) # Ratios
        worksheet.set_column(5, 9, 2)  # Spacer (F-J hidden or narrow)
        worksheet.set_column(10, 20, 15) # Raw Data

        workbook.close()
        output.seek(0)
        
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name="screener_ratios.xlsx"
        )
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run()
