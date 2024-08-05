import json
from flask import Flask
from flask import jsonify, Response, Request
from flask import request

import weather_snow_session

session = weather_snow_session.get_client()

app = Flask(__name__)

@app.route('/weather', methods=["GET"])
def get_weather():
    try:
        df = session.table('TEST.PUBLIC.WeatherForecast_14Day')
        output = df.to_dict()
    except Exception as e:
        return e
    return jsonify(output)

if __name__ == "__main__":
    app.run(debug=True)
    
