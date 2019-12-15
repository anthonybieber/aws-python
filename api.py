from blueprints.s3 import s3
from blueprints.kinesisa import kinesis

from flask import Flask
app = Flask(__name__)

app.register_blueprint(s3)
app.register_blueprint(kinesis)

if __name__ == '__main__':
    app.run()
