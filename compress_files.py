import os
import json
import gzip

def compress_user_uploads():
    folder = 'user_uploads'
    
    for filename in os.listdir(folder):
        if filename.endswith('.json'):
            filepath = os.path.join(folder, filename)
            
            print(f"Compressing {filename}...")
            
            # Lê JSON original
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Grava comprimido
            compressed_path = filepath + '.gz'
            with gzip.open(compressed_path, 'wt', encoding='utf-8') as f:
                json.dump(data, f)
            
            # Confirma tamanho
            original_size = os.path.getsize(filepath)
            compressed_size = os.path.getsize(compressed_path)
            reduction = (1 - compressed_size/original_size) * 100
            
            print(f"  Original: {original_size/1024/1024:.2f} MB")
            print(f"  Compressed: {compressed_size/1024/1024:.2f} MB")
            print(f"  Reduction: {reduction:.1f}%")
            
            # APAGA original (cuidado!)
            os.remove(filepath)
            print(f"  ✓ Removed original\n")

if __name__ == '__main__':
    compress_user_uploads()
    print("✅ All files compressed!")
