from langchain_ollama import ChatOllama

from chatbot.src import config


def get_llm() -> ChatOllama:
    """Devuelve una instancia configurada de ChatOllama."""
    return ChatOllama(
        model=config.OLLAMA_MODEL,
        temperature=0,
    )
