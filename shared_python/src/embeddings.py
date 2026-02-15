import datetime
from sentence_transformers import SentenceTransformer

from . import loadenv

def createEmbedding(text: str) -> list[float]:

    start_time = datetime.datetime.now()
    preview_text = text[:text.find("\n")] if "\n" in text else text[:30]
    print(f"Creating embedding for text: {preview_text}...") # Print the first line of the text for context

    model_name = loadenv.loadEnvVariable("EMBEDDING_MODEL")
    model_dimensions = int(loadenv.loadEnvVariable("EMBEDDING_DIMENSIONS"))
    try:
        hf_token = loadenv.loadEnvVariable("HUGGING_FACE_TOKEN")
    except EnvironmentError:
        hf_token = None

    model = SentenceTransformer(model_name, trust_remote_code=True, token=hf_token)
    embedding = model.encode(text)

    end_time = datetime.datetime.now()
    elapsed_time = (end_time - start_time).total_seconds()
    print(f"Embedding created in {elapsed_time:.2f} seconds.")
    return embedding.tolist()[:model_dimensions] # Trim the embedding to the specified dimensions, if necessary.