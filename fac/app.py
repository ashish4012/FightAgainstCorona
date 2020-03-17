from flask import Flask, render_template, make_response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

from fac import config
from fac.core.database import FacDB
import io
import numpy as  np

app = Flask(__name__, template_folder='static/templates')


# main route
@app.route("/")
def index():
    demo_data = conn.get_state_demographics_date("Delhi")
    return render_template('index.html', **demo_data[0])


@app.route('/plot/states')
def plot_temp():
    demo_data = conn.get_state_demographics_date("Delhi")
    ys = demo_data[0]
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    axis.set_title("COVID 19")
    axis.set_xlabel("People")
    axis.grid(True)
    xs = range(8)
    axis.plot(xs, ys)
    canvas = FigureCanvas(fig)
    output = io.BytesIO()
    canvas.print_png(output)
    response = make_response(output.getvalue())
    response.mimetype = 'image/png'
    return response


if __name__ == "__main__":
    conn = FacDB('SQLITE')
    conn.create_db_tables()
    app.run(debug=config.DEBUG)
