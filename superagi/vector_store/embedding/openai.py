import openai

from superagi.config.config import get_config


class OpenAiEmbedding:
    def __init__(self, api_key, model=None):
        self.model = model or get_config("OPENAI_EMBEDDING_MODEL", "text-embedding-ada-002")
        self.api_key = api_key

    async def get_embedding_async(self, text: str):
        try:
            response = await openai.Embedding.create(
                api_key=self.api_key,
                api_base=get_config("OPENAI_API_BASE", "https://api.openai.com/v1"),
                input=[text],
                model=self.model
            )
            return response['data'][0]['embedding']
        except Exception as exception:
            return {"error": exception}


    def get_embedding(self, text):
        try:
            response = openai.Embedding.create(
                api_key=self.api_key,
                api_base=get_config("OPENAI_API_BASE", "https://api.openai.com/v1"),
                input=[text],
                model=self.model
            )
            return response['data'][0]['embedding']
        except Exception as exception:
            return {"error": exception}
