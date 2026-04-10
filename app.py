import streamlit as st
import pandas as pd
import time
import re
import openpyxl

# Configure the Streamlit page
st.set_page_config(
    page_title="Phone Number Filter", 
    page_icon="", 
    layout="wide"
)

def is_empty(val):
    """Check if a cell value is considered empty."""
    if pd.isna(val) or val is None:
        return True
    if str(val).strip() == '':
        return True
    return False

def process_number(num_str):
    """
    Extracts and validates a 10-digit mobile number.
    Returns the 10-digit number or None if invalid.
    """
    num_str = str(num_str)
    
    # Remove all non-digit characters (like spaces, dashes, etc.)
    digits = re.sub(r'\D', '', num_str)
    
    # Valid mobile numbers must be EXACTLY 10 digits and start with 6, 7, 8, or 9
    if len(digits) == 10 and digits[0] in '6789':
        return digits
        
    return None

def process_csv_chunked(file, remove_duplicates, progress_text):
    """
    Memory optimized CSV processing using pandas chunk_size.
    We iterate over chunks of data instead of loading the entire file.
    """
    valid_numbers = set()
    total_rows = 0
    total_valid = 0
    total_invalid = 0
    
    output_rows = []
    headers = None
    
    # Using low_memory=False to prevent mixed type warnings and chunksize to avoid memory overload
    chunk_iter = pd.read_csv(file, chunksize=50000, dtype=str, on_bad_lines='skip', low_memory=False)
    
    for chunk in chunk_iter:
        if headers is None:
            headers = list(chunk.columns)
            
        total_rows += len(chunk)
        
        # Update progress text
        progress_text.text(f"Processing... {total_rows:,} rows scanned.")
        
        for row_tuple in chunk.itertuples(index=False, name=None):
            valid_number_found = None
            for val in row_tuple:
                if is_empty(val):
                    continue
                cleaned = process_number(val)
                if cleaned:
                    valid_number_found = cleaned
                    break
            
            if valid_number_found:
                if remove_duplicates:
                    if valid_number_found not in valid_numbers:
                        valid_numbers.add(valid_number_found)
                        output_rows.append(row_tuple)
                        total_valid += 1
                else:
                    output_rows.append(row_tuple)
                    total_valid += 1
            else:
                total_invalid += 1
                
    return output_rows, headers, total_rows, total_valid, total_invalid

def process_excel_iterative(file, remove_duplicates, progress_text):
    """
    Memory optimized Excel processing using openpyxl in read-only mode.
    Generator-based iteration prevents holding entire rows/sheets in memory.
    """
    valid_numbers = set()
    total_rows = 0
    total_valid = 0
    total_invalid = 0
    
    output_rows = []
    headers = None
    
    try:
        # read_only=True ensures memory efficient row-by-row iteration without loading whole document
        wb = openpyxl.load_workbook(file, read_only=True, data_only=True)
        for sheet_index, sheet_name in enumerate(wb.sheetnames):
            ws = wb[sheet_name]
            
            # Use values_only=True to get raw values directly, skipping cell objects metadata (much faster/lighter)
            for row_idx, row in enumerate(ws.iter_rows(values_only=True)):
                total_rows += 1
                
                if headers is None and sheet_index == 0 and row_idx == 0:
                    headers = [f"Col_{i+1}" if is_empty(col) else str(col) for i, col in enumerate(row)]
                    continue
                elif sheet_index > 0 and row_idx == 0:
                    continue
                
                # Update UI periodically to prevent slowing down the loop
                if total_rows % 5000 == 0:
                    progress_text.text(f"Processing... {total_rows:,} rows scanned.")
                    
                valid_number_found = None
                for val in row:
                    if is_empty(val):
                        continue
                        
                    cleaned = process_number(val)
                    if cleaned:
                        valid_number_found = cleaned
                        break
                        
                if valid_number_found:
                    if remove_duplicates:
                        if valid_number_found not in valid_numbers:
                            valid_numbers.add(valid_number_found)
                            output_rows.append(row)
                            total_valid += 1
                    else:
                        output_rows.append(row)
                        total_valid += 1
                else:
                    total_invalid += 1
        wb.close()
    except Exception as e:
        raise RuntimeError(f"Failed to read Excel file appropriately. Make sure the file isn't corrupted. Error details: {str(e)}")
        
    if not headers and output_rows:
        headers = [f"Col_{i}" for i in range(len(output_rows[0]))]
        
    return output_rows, headers, total_rows, total_valid, total_invalid

def main():
    st.title("Indian Mobile Number Extractor")
    st.markdown("Upload large CSV or Excel files (up to **600MB**) to extract valid Indian mobile phone numbers efficiently. Processing is optimized to prevent out-of-memory crashes.")
    
    # Instructions for 600MB limit configuration
    st.sidebar.markdown("### Upload Instructions")
    st.sidebar.info(
        "**Note on File Size Limit:**\n\n"
        "Streamlit sets a default upload limit of 200MB.\n"
        "To allow uploads up to **600MB**, you must run this app using:\n\n"
        "`streamlit run app.py --server.maxUploadSize 600`"
    )
    
    st.sidebar.header("Processing Settings")
    remove_duplicates = st.sidebar.checkbox("Remove Duplicate Numbers", value=True, help="Toggle to filter out identical numbers across all columns.")
    
    # File uploader
    uploaded_file = st.file_uploader("Choose a CSV or Excel file", type=['csv', 'xlsx'])
    
    if uploaded_file is not None:
        file_size_mb = uploaded_file.size / (1024 * 1024)
        
        # Server-side validation just in case the limit was bypassed
        if file_size_mb > 600:
            st.error(f"File size limit exceeded! Your file is {file_size_mb:.2f}MB. Please upload a file smaller than 600MB.")
            return
            
        # UI controls and processing triggers
        col_btn, _ = st.columns([1, 4])
        with col_btn:
            start_process = st.button("Process Data", use_container_width=True)
            
        if start_process:
            start_time = time.time()
            
            # Placeholder for progress
            progress_bar = st.progress(0)
            progress_text = st.empty()
            
            try:
                progress_bar.progress(10) # Initial progress state
                progress_text.text("Starting extraction...")
                st.session_state.processed_data = None # Reset previous outputs
                
                # Check extension and route to specific memory optimized parser
                is_csv = uploaded_file.name.lower().endswith('.csv')
                
                if is_csv:
                    results = process_csv_chunked(uploaded_file, remove_duplicates, progress_text)
                else:
                    results = process_excel_iterative(uploaded_file, remove_duplicates, progress_text)
                    
                output_rows, headers, total_rows_proc, total_valid, total_invalid = results
                
                processing_time = time.time() - start_time
                
                progress_bar.progress(100)
                progress_text.empty()
                st.success(f"Processing Complete in {processing_time:.2f} seconds!")
                
                # Render Metrics
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Rows Processed", f"{total_rows_proc:,}")
                col2.metric("Valid Rows", f"{total_valid:,}")
                col3.metric("Invalid Rows Removed", f"{total_invalid:,}")
                
                valid_percent = 0
                if (total_valid + total_invalid) > 0:
                    valid_percent = (total_valid / (total_valid + total_invalid)) * 100
                col4.metric("Valid Ratio", f"{valid_percent:.2f}%")
                
                if len(output_rows) > 0:
                    df_out = pd.DataFrame(output_rows, columns=headers)
                    
                    st.divider()
                    st.subheader("Preview (First 100 Entries)")
                    st.dataframe(df_out.head(100), use_container_width=True)
                    
                    # Store valid list internally and generate bytes for UI
                    csv_data = df_out.to_csv(index=False).encode('utf-8')
                    
                    st.download_button(
                        label="Download Cleaned File as CSV",
                        data=csv_data,
                        file_name=f'cleaned_file_{int(time.time())}.csv',
                        mime='text/csv',
                        type="primary"
                    )
                else:
                    st.warning("No valid Indian phone numbers were found in the file.")
                    
            except Exception as e:
                progress_bar.empty()
                progress_text.empty()
                st.error(f"An error occurred during processing:\n{str(e)}")

if __name__ == "__main__":
    main()
