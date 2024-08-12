from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/dealfinder', methods=['POST'])
def dealfinder():
    if request.method == 'POST':
        req_data = request.json # This is the data being posted by the request, should be city, min and max price.
        
        status = "" # Use number status as numbered codes or do both the code and the actual string status.
        data = "" # This is the data to be returned to the requester.
        
        # Not sure how you want to execute this but here is where you would do it.
        
        
        # Return the data as a JSON object
        return jsonify({
                "status": status,
                "data": data,
                "request_data": req_data
            })
        
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8443)