document.addEventListener('DOMContentLoaded', function() {
    setupDropZone('dropZone1', 'fileInput1', '/convert-to-feather');
    setupDropZone('dropZone2', 'fileInput2', '/convert-from-feather');
});

function setupDropZone(dropZoneId, fileInputId, endpoint) {
    const dropZone = document.getElementById(dropZoneId);
    const fileInput = document.getElementById(fileInputId);
    const progressModal = new bootstrap.Modal(document.getElementById('progressModal'));

    dropZone.addEventListener('click', () => fileInput.click());

    dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        dropZone.classList.add('dragover');
    });

    dropZone.addEventListener('dragleave', () => {
        dropZone.classList.remove('dragover');
    });

    dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        dropZone.classList.remove('dragover');
        const files = e.dataTransfer.files;
        if (files.length) {
            handleFile(files[0], endpoint, dropZone, progressModal);
        }
    });

    fileInput.addEventListener('change', (e) => {
        if (e.target.files.length) {
            handleFile(e.target.files[0], endpoint, dropZone, progressModal);
        }
    });
}

function handleFile(file, endpoint, dropZone, progressModal) {
    const originalContent = dropZone.innerHTML;
    const progressText = document.getElementById('progressText');
    
    // Show progress modal
    progressModal.show();
    progressText.textContent = `Memproses ${file.name}...`;

    const formData = new FormData();
    formData.append('file', file);

    fetch(endpoint, {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            return response.text().then(text => {
                throw new Error(text || 'Konversi gagal');
            });
        }
        return response.blob();
    })
    .then(blob => {
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = endpoint.includes('to-feather') ? 
            file.name + '.parquet' : 
            file.name.replace('.parquet', '.csv');
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        a.remove();
        
        // Show success message
        progressText.textContent = 'Konversi berhasil! Mengunduh file...';
        setTimeout(() => {
            progressModal.hide();
            dropZone.innerHTML = originalContent;
        }, 1500);
    })
    .catch(error => {
        progressModal.hide();
        alert('Error: ' + error.message);
        dropZone.innerHTML = originalContent;
    });
}

// Update MIME type handling
function getMimeType(filename) {
    if (filename.endsWith('.parquet')) {
        return 'application/octet-stream';
    } else if (filename.endsWith('.feather')) {
        return 'application/octet-stream';
    } else if (filename.endsWith('.csv')) {
        return 'text/csv';
    }
    return 'application/octet-stream';
} 