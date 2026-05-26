import pandas as pd
import gradio as gr

from chatbot.src.athena.executor import run_query
from chatbot.src.llm.generator import generate_sql
from chatbot.src.sql.validator import ValidationError

_EMPTY_DF = pd.DataFrame()


def answer(question: str) -> tuple[str, pd.DataFrame]:
    if not question.strip():
        return "Escribe una pregunta antes de generar el SQL.", _EMPTY_DF
    try:
        sql = generate_sql(question)
    except ValidationError as e:
        return f"Error de validación: {e}", _EMPTY_DF
    except Exception as e:
        return f"Error inesperado: {e}", _EMPTY_DF
    try:
        df = run_query(sql)
        return sql, df
    except Exception as e:
        return sql, pd.DataFrame({"error": [str(e)]})


with gr.Blocks(title="Fire Risk Chatbot") as demo:
    gr.Markdown("# Fire Risk Chatbot")
    gr.Markdown("Haz una pregunta en castellano sobre los datos de riesgo de incendio de Galicia.")

    with gr.Row():
        question = gr.Textbox(
            label="Pregunta",
            placeholder="¿Cuál fue el día con mayor riesgo de incendio en Vigo en 2024?",
            lines=2,
        )

    with gr.Row():
        btn = gr.Button("Generar SQL", variant="primary")

    with gr.Row():
        sql_output = gr.Code(
            label="SQL generado",
            language="sql",
            interactive=False,
        )

    with gr.Row():
        results_output = gr.Dataframe(
            label="Resultados",
            interactive=False,
        )

    btn.click(fn=answer, inputs=question, outputs=[sql_output, results_output])
    question.submit(fn=answer, inputs=question, outputs=[sql_output, results_output])


if __name__ == "__main__":
    demo.launch()
