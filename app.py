from flask import Flask, render_template, request, send_file, after_this_request
import pandas as pd
import os
from werkzeug.utils import secure_filename
import glob

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'

# Pastikan folder uploads ada
if not os.path.exists(app.config['UPLOAD_FOLDER']):
    os.makedirs(app.config['UPLOAD_FOLDER'])

def cleanup_uploads():
    """Membersihkan semua file di folder uploads"""
    files = glob.glob(os.path.join(app.config['UPLOAD_FOLDER'], '*'))
    for f in files:
        try:
            os.remove(f)
        except:
            pass

def remove_file(path):
    """Fungsi untuk menghapus file"""
    try:
        os.remove(path)
    except:
        pass

@app.route('/')
def index():
    cleanup_uploads()
    return render_template('index.html')

@app.route('/convert-to-feather', methods=['POST'])
def convert_to_feather():
    cleanup_uploads()
    
    if 'file' not in request.files:
        return 'Tidak ada file yang diunggah', 400
    
    file = request.files['file']
    if file.filename == '':
        return 'Tidak ada file yang dipilih', 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    try:
        # Baca file dengan pengaturan yang lebih ketat
        if filename.endswith('.csv'):
            df = pd.read_csv(
                filepath,
                low_memory=False,  # Mencegah inferensi tipe data yang salah
                dtype='object',    # Membaca semua kolom sebagai string dulu
                na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', 
                          '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A', 
                          'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']  # Menangani berbagai format NA
            )
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(
                filepath,
                dtype='object',    # Membaca semua kolom sebagai string dulu
                na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN',
                          '-NaN', '-nan', '1.#IND', '1.#QNAN', '<NA>', 'N/A',
                          'NA', 'NULL', 'NaN', 'n/a', 'nan', 'null']
            )
        else:
            remove_file(filepath)
            return 'Format file tidak didukung', 400

        # Konversi tipe data yang sesuai setelah membaca
        for column in df.columns:
            # Coba konversi ke numeric jika memungkinkan
            try:
                if df[column].dtype == 'object':
                    numeric_series = pd.to_numeric(df[column], errors='raise')
                    df[column] = numeric_series
            except:
                # Jika gagal konversi numeric, biarkan sebagai string
                pass

        output_filename = filename + '.parquet'
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        
        # Simpan ke parquet dengan pengaturan yang lebih aman
        df.to_parquet(
            output_path,
            engine='pyarrow',     # Menggunakan engine pyarrow yang lebih stabil
            compression='zstd',
            compression_level=22,
            row_group_size=500000,
            use_dictionary=True,
            dictionary_pagesize_limit=2097152
        )
        
        remove_file(filepath)

        @after_this_request
        def cleanup(response):
            try:
                remove_file(output_path)
            except:
                pass
            return response
        
        return send_file(
            output_path,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/octet-stream'
        )

    except Exception as e:
        cleanup_uploads()
        return f'Terjadi kesalahan dalam pemrosesan file: {str(e)}', 400

@app.route('/convert-from-feather', methods=['POST'])
def convert_from_feather():
    cleanup_uploads()
    
    if 'file' not in request.files:
        return 'Tidak ada file yang diunggah', 400
    
    file = request.files['file']
    if file.filename == '':
        return 'Tidak ada file yang dipilih', 400

    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    file.save(filepath)

    try:
        # Baca parquet dengan pengaturan yang lebih aman
        df = pd.read_parquet(filepath, engine='pyarrow')
        
        output_filename = filename.replace('.parquet', '') + '.csv'
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        
        # Simpan ke CSV dengan pengaturan yang lebih aman
        df.to_csv(
            output_path,
            index=False,
            date_format='%Y-%m-%d %H:%M:%S',  # Format tanggal yang konsisten
            float_format='%.10g',             # Presisi floating point yang lebih tinggi
            encoding='utf-8',                 # Encoding yang eksplisit
            na_rep='',                        # Representasi NA yang konsisten
            quoting=1                         # Mengutip semua field non-numerik
        )
        
        remove_file(filepath)

        @after_this_request
        def cleanup(response):
            try:
                remove_file(output_path)
            except:
                pass
            return response
        
        return send_file(
            output_path,
            as_attachment=True,
            download_name=output_filename,
            mimetype='text/csv'
        )

    except Exception as e:
        cleanup_uploads()
        return f'Terjadi kesalahan: {str(e)}', 400

if __name__ == '__main__':
    cleanup_uploads()
    app.run(debug=True) 