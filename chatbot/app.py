import gradio as gr

from chatbot.src.llm.generator import generate_sql
from chatbot.src.sql.validator import ValidationError


def answer(question: str) -> str:
    if not question.strip():
        return "Escribe una pregunta antes de generar el SQL."
    try:
        return generate_sql(question)
    except ValidationError as e:
        return f"Error de validación: {e}"
    except Exception as e:
        return f"Error inesperado: {e}"


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

    btn.click(fn=answer, inputs=question, outputs=sql_output)
    question.submit(fn=answer, inputs=question, outputs=sql_output)


if __name__ == "__main__":
    demo.launch()
