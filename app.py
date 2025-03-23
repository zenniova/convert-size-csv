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
        # Baca file
        if filename.endswith('.csv'):
            df = pd.read_csv(filepath)
        elif filename.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(filepath)
        else:
            remove_file(filepath)
            return 'Format file tidak didukung', 400

        output_filename = filename + '.parquet'
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        
        # Simpan ke parquet
        df.to_parquet(
            output_path,
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
        # Baca parquet
        df = pd.read_parquet(filepath)
        
        output_filename = filename + '.csv'
        output_path = os.path.join(app.config['UPLOAD_FOLDER'], output_filename)
        
        # Simpan ke CSV
        df.to_csv(
            output_path,
            index=False
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