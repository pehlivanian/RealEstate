from flask import Flask, render_template, request, redirect, url_for
import asyncio
from proto import PropertyAggregator
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/properties', methods=['GET', 'POST'])
def properties():
    if request.method == 'POST':
        city = request.form.get('city', '').strip()
        state = request.form.get('state', '').strip().upper()
        
        if not city or not state:
            return render_template('index.html', error="Please enter both city and state")
        
        # Use the API key from proto.py
        api_key = "9cd0949b08msha286b6987d46fe9p17feccjsn846e7f222f6f"
        
        try:
            # Create aggregator and fetch properties
            aggregator = PropertyAggregator(api_key, city, state)
            logger.info(f"Fetching properties for {city}, {state}")
            
            # Run async function in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(aggregator.fetch_all_properties())
            loop.close()
            
            return render_template('properties.html', 
                                 properties=aggregator.properties,
                                 city=city, 
                                 state=state)
                                 
        except Exception as e:
            logger.error(f"Error fetching properties: {str(e)}")
            return render_template('index.html', error=f"Error fetching properties: {str(e)}")
    
    # GET request - redirect to home
    return redirect(url_for('index'))

if __name__ == '__main__':
    import os
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)