import math
import os
import numpy as np
import pandas as pd
import dask
import dask.dataframe as dd
import requests
from dotenv import load_dotenv
import dask.array as da
from dask.distributed import Client
from dask.diagnostics import ProgressBar

class SimilarityMatrixDask:
    def __init__(self, data_path, output_path, memory_limit, n_workers=1, threads_per_worker=1, error_api_url=None):
        self.data_path = data_path
        self.output_path = output_path
        self.memory_limit = memory_limit
        self.n_workers = n_workers
        self.threads_per_worker = threads_per_worker
        self.error_api_url = error_api_url

    def setup_client(self):
        """Setup Dask distributed client"""
        self.client = Client(
            n_workers=self.n_workers, 
            threads_per_worker=self.threads_per_worker, 
            memory_limit=self.memory_limit
        )
        print("✅ Dask client initialized")
        print(self.client)
    
    def report_error(self, error_meassage):
        if not self.error_api_url:
            return
        error_data = {"error_message": error_meassage}
        
        try:
            api_response = requests.post(self.error_api_url, json=error_data)
            if api_response.status_code == 200:
                print("Error reported successfully to the API")
            else:
                print(f"Failed to report error to the API. Status code: {api_response.status_code}")
        except Exception as api_e:
            print(f"Error calling the error reporting API: {api_e}")

    def compute_and_save_similarity_matrix(self):
        try:
            print("🔹 Reading Parquet data efficiently...")
            
            # Load all partitions at once for better efficiency
            ddf = dd.read_parquet(self.data_path, engine='pyarrow')

            # Set index (avoid re-sorting as it takes extra time)
            ddf = ddf.set_index("domain_")

            # Extract domain names (avoiding full compute where possible)
            domains = ddf.index.compute()  # Avoid converting to list immediately

            # Define a delayed function to stack a partition
            def stack_partition(series):
                return np.stack(series)

            # Convert each partition of the "vector" column into a delayed NumPy array
            delayed_parts = ddf["feature_vector"].to_delayed()
            delayed_arrays = [dask.delayed(stack_partition)(part) for part in delayed_parts]
            # Wrap each delayed array as a Dask array; note: use shape=(NaN, feature_dim)
            arrays = [da.from_delayed(d, shape=(np.nan, 319949), dtype=np.float32) for d in delayed_arrays]

            # Concatenate all arrays along axis 0 to form the full array of vectors
            vectors_da = da.concatenate(arrays, axis=0)

            # Make sure chunk sizes are computed
            vectors_da.compute_chunk_sizes()
            num_chunks = len(vectors_da.chunks[0])
            chunk_size = vectors_da.chunks[0][0]
            chunk_mem_size = (chunk_size * 319949 * 4) / (1024**3)  # in GB
            print(f"📌 Number of chunks: {num_chunks}")
            print(f"📌 Chunk size: {chunk_size} vectors, approx {chunk_mem_size:.2f} GB per chunk")

            # Normalize the vectors (L2 norm) for cosine similarity computation
            norms = da.linalg.norm(vectors_da, axis=1, keepdims=True)
            vectors_norm = vectors_da / da.maximum(norms, 1e-8)
            vectors_norm = vectors_norm.persist()

            print("🔹 Computing Cosine Similarity Matrix...")
            similarity_matrix = da.dot(vectors_norm, vectors_norm.T)

            # Optimize chunking strategy
            similarity_matrix = similarity_matrix.rechunk((5000, 5000))  # Adjust based on memory limits

            # Persist to ensure efficient computation
            similarity_matrix = similarity_matrix.persist()

            print(f"📌 Similarity matrix shape: {similarity_matrix.shape}")

            # Convert Dask array to a DataFrame efficiently
            df_similarity = dd.from_array(similarity_matrix, columns=domains)

            # Add the index column
            df_similarity["domain_"] = pd.Series(domains)

            print("🔹 Writing Similarity Matrix to Parquet...")
            with ProgressBar():
                df_similarity.to_parquet(self.output_path, engine="pyarrow", overwrite=True)

            print("✅ Similarity Matrix computation and save completed!")

            error_message = str("✅ Similarity Matrix computation and save completed!")
            self.report_error(error_message)
        
        except Exception as e:
            error_message = str(e)
            # print("")
            self.report_error(error_message)


if __name__ == "__main__":
    '''
    Tính toán simlarity matrix trên một tập dữ liệu lớn hơn RAM, sử dụng dask để quản lý các worker và RAM (distributed computing).

    Args:
        data_path (str): Data path cần tính toán.
        output_path (str): Output của similarity matrix sau khi tính toán xong.
        memory_limit (int): Số lượng RAM tối đa 1 worker có thể sử dụng.
        n_worker (int): Số lượng worker dùng để tính toán.
        thread_per_worker (int): Số lượng thread mỗi worker.
    
    Returns:
        Similarity matrix được chia thành các partition.
    '''

    load_dotenv()
    #Configuration (adjust paths as needed)
    data_path = os.getenv("DATA_PATH")
    output_path = os.getenv("OUTPUT_PATH")
    memory_limit = os.getenv("MEMORY_LIMIT")
    n_workers = int(os.getenv("N_WORKERS", 1))
    threads_per_worker = int(os.getenv("THREADS_PER_WORKER", 1))
    error_api_url = os.getenv("ERROR_API_URL")

    smc = SimilarityMatrixDask(data_path, output_path, memory_limit, n_workers, threads_per_worker, error_api_url)
    smc.setup_client()
    smc.compute_and_save_similarity_matrix()
