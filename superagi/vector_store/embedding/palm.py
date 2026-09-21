try:
    import google.generativeai as palm
except ImportError:  # google-generativeai requires Python>=3.9; defer failure until actually used
    palm = None

import openai


class PalmEmbedding:
    def __init__(self, api_key, model="models/embedding-gecko-001"):
        if palm is None:
            raise ImportError("google-generativeai is not installed (requires Python>=3.9)")
        self.model = model
        self.api_key = api_key

    def get_embedding(self, text):
        try:
            response = palm.generate_embeddings(model=self.model, text=text)
            return response['embedding']
        except Exception as exception:
            return {"error": exception}
