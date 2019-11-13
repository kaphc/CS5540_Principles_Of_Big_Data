import pyrebase

config = {
    "apiKey": "AIzaSyBxc8uvDbRpnRCRJSQKTiCu6TZiHWHxBy0",
    "authDomain": "tweeflix.firebaseapp.com",
    "databaseURL": "https://tweeflix-sentiment.firebaseio.com",
    "projectId": "tweeflix",
    "storageBucket": "tweeflix.appspot.com",
    "messagingSenderId": "459096245616",
    "appId": "1:459096245616:web:268d75916b13a458f03bb5",
    "measurementId": "G-S3BZVK77XD"
}

# initialize the fire base
firebase = pyrebase.initialize_app(config)

# initialize the database
db = firebase.database()
s = "key1"
db.update({ s:s})



