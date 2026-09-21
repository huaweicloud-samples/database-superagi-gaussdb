"""本地 LLM（Ollama 等 OpenAI 兼容端点）的模型名不在 llama_index 0.6.x 的
OpenAI 模型白名单（ALL_AVAILABLE_MODELS）内，其 token 上下文探测会对未知
模型名抛 ValueError，导致 QueryResource/摘要等 llama_index 链路直接失败。

在进程启动时调用 patch_llama_index_model_whitelist()，把配置中的本地模型名
（MODEL_NAME / RESOURCES_SUMMARY_MODEL_NAME）注入白名单，上下文窗口取
MAX_MODEL_TOKEN_LIMIT 配置（默认 8192）。
"""
from superagi.config.config import get_config
from superagi.lib.logger import logger

DEFAULT_CONTEXT_SIZE = 8192


def patch_llama_index_model_whitelist() -> None:
    """把本地/自定义模型名注入 llama_index 的 OpenAI 模型白名单。幂等。"""
    try:
        from llama_index.llm_predictor import openai_utils
    except ImportError:
        return

    names = {get_config("MODEL_NAME"), get_config("RESOURCES_SUMMARY_MODEL_NAME")}
    context_size = get_config("MAX_MODEL_TOKEN_LIMIT", DEFAULT_CONTEXT_SIZE) or DEFAULT_CONTEXT_SIZE
    for name in names:
        if not name or name in openai_utils.ALL_AVAILABLE_MODELS:
            continue
        try:
            openai_utils.ALL_AVAILABLE_MODELS[name] = int(context_size)
            logger.info(f"llama_index model whitelist: injected {name} (context={context_size})")
        except Exception as e:
            logger.warning(f"llama_index model whitelist patch skipped for {name}: {e}")
