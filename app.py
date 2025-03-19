from shiny.express import input, render, ui

ui.input_text("name", "What's your name?", value="World")

@render.text
def greeting():
    return f"Hello, {input.name()}!"