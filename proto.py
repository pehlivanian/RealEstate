import aiohttp
import asyncio
import json
import http.client
import urllib.parse
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
import numpy as np
from typing import Dict, List, Any
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PropertyAggregator:
    def __init__(self, rapid_api_key: str):
        self.api_key = rapid_api_key
        self.properties = []
        self.apis = {
            'us-real-estate': {
                'host': 'us-real-estate.p.rapidapi.com',
                'endpoint': '/v2/for-sale',
                'params': {
                    'city': 'Nyack',
                    'state_code': 'NY',
                    'offset': '0',
                    'limit': '200',
                    'sort': 'newest',
                }
            },
            'zillow': {
                'host': 'zillow-com1.p.rapidapi.com',
                'endpoint': '/propertyExtendedSearch',
                'params': {
                    'location': 'Nyack, NY',
                    'status_type': 'ForSale',
                    'home_type': 'Houses'
                }
            }
        }

    def parse_zillow_data(self, data: Dict) -> List[Dict]:
        """
        Parse Zillow Extended Search API data for properties
        """
        parsed_properties = []
    
        try:
            # Log the full data structure for debugging
            logger.info("Zillow API Full Data:")
            logger.info(json.dumps(data, indent=2))
        
            # Extract properties from the search results
            properties = data.get('props', [])
        
            for prop in properties:
                try:
                    # Extract property details
                    address = prop.get('address', 'N/A')
                    price = prop.get('price', 0)
                    bedrooms = prop.get('beds', 0)
                    bathrooms = prop.get('baths', 0)
                    sqft = prop.get('area', 0)
                
                    # More robust image handling
                    thumbnail_url = None
                
                    # Try multiple possible image sources
                    image_sources = [
                        prop.get('image'),
                        prop.get('photo'),
                        prop.get('primary_photo', {}).get('href'),
                        prop.get('images', [{}])[0].get('href') if prop.get('images') else None
                        ]

                    # Use imgSrc for primary image
                    thumbnail_url = prop.get('imgSrc')
                    if not thumbnail_url:
                        for img in image_sources:
                            if img and isinstance(img, str) and 'placeholder' not in img.lower():
                                thumbnail_url = img
                                break
                
                    # If no valid image found, use a descriptive placeholder
                    if not thumbnail_url:
                        thumbnail_url = f"https://via.placeholder.com/200x150.png?text={urllib.parse.quote(f'{bedrooms} Bed {bathrooms} Bath')}"
                
                    # Listing URL
                    listing_url = prop.get('url')
                    if not listing_url:
                        # Generate a Zillow search URL as a fallback
                        listing_url = f"https://www.zillow.com/homes/{urllib.parse.quote(address.replace(' ', '-'))}"
                
                    # Estimate monthly costs 
                    monthly_costs = self.estimate_monthly_costs(price, sqft, 'single family')
                
                    # Add property to parsed list
                    parsed_properties.append({
                        'price': price,
                        'address': address,
                        'bedrooms': bedrooms,
                        'bathrooms': bathrooms,
                        'sqft': sqft,
                        'property_type': 'Single Family',
                        'thumbnail_url': thumbnail_url,
                        'monthly_costs': monthly_costs,
                        'neighborhood': prop.get('neighborhood', 'Nyack'),
                        'listing_url': listing_url,
                        'source': 'Zillow'
                        })
                
                    # Log each parsed property for verification
                    logger.info(f"Parsed Zillow Property: {address}, Price: ${price}, Thumbnail: {thumbnail_url}")
            
                except Exception as e:
                    logger.error(f"Error parsing individual Zillow property: {str(e)}")
                    continue

            logger.info(f"Successfully parsed {len(parsed_properties)} Zillow properties")
    
        except Exception as e:
            logger.error(f"Error parsing Zillow properties: {str(e)}")
            return []
    
        return parsed_properties

    async def fetch_data(self, session: aiohttp.ClientSession, api_name: str) -> List[Dict]:
        # For Zillow, we'll use a different connection method due to specific API requirements
        if api_name == 'zillow':
            try:
                headers = {
                    'x-rapidapi-key': self.api_key,
                    'x-rapidapi-host': self.apis[api_name]['host']
                }

                # Construct query string
                query_params = urllib.parse.urlencode(self.apis[api_name]['params'])
                endpoint = f"{self.apis[api_name]['endpoint']}?{query_params}"

                conn = http.client.HTTPSConnection(self.apis[api_name]['host'])
                conn.request("GET", endpoint, headers=headers)
                
                res = conn.getresponse()
                if res.status == 200:
                    data = json.loads(res.read().decode("utf-8"))
                    logger.info(f"Successfully fetched data from {api_name}")
                    return self.parse_data(api_name, data)
                else:
                    logger.error(f"Error fetching data from {api_name}: {res.status}")
                    return []
            except Exception as e:
                logger.error(f"Exception while fetching {api_name} data: {str(e)}")
                return []
        
        # Existing implementation for other APIs
        else:
            headers = {
                'X-RapidAPI-Key': self.api_key,
                'X-RapidAPI-Host': self.apis[api_name]['host']
            }
            
            url = f"https://{self.apis[api_name]['host']}{self.apis[api_name]['endpoint']}"
            
            try:
                async with session.get(url, headers=headers, params=self.apis[api_name]['params']) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Successfully fetched data from {api_name}")
                        return self.parse_data(api_name, data)
                    else:
                        response_text = await response.text()
                        logger.error(f"Error fetching data from {api_name}: {response.status}")
                        logger.error(f"Response: {response_text}")
                        return []
            except Exception as e:
                logger.error(f"Exception while fetching {api_name} data: {str(e)}")
                return []            

    def estimate_monthly_costs(self, price: float, sqft: float, property_type: str) -> Dict[str, float]:
        """
        Estimate monthly non-mortgage costs
        """
        # Ensure inputs are not None
        price = float(price or 500000)
        sqft = float(sqft or 1500)
        property_type = str(property_type or 'Unknown').lower()

        # Calculate annual costs
        annual_property_tax = price * 0.02
        annual_insurance = price * 0.005
        annual_utilities = sqft * 2.0
        
        # HOA for condos/apartments
        annual_hoa = price * 0.004 if 'condo' in property_type or 'apartment' in property_type else 0
        
        annual_misc = price * 0.001
        annual_municipal = price * 0.0005

        # Monthly breakdown
        monthly_costs = {
            'property_tax': round(annual_property_tax / 12, 2),
            'insurance': round(annual_insurance / 12, 2),
            'utilities': round(annual_utilities / 12, 2),
            'hoa_maintenance': round(annual_hoa / 12, 2),
            'misc_expenses': round(annual_misc / 12, 2),
            'municipal_services': round(annual_municipal / 12, 2),
            'total_monthly_non_mortgage_costs': round(
                (annual_property_tax + annual_insurance + 
                 annual_utilities + annual_hoa + 
                 annual_misc + annual_municipal) / 12, 2
            )
        }
        
        return monthly_costs

    def determine_neighborhood(self, lat: float, lon: float) -> str:
        """
        Determine neighborhood based on geographic coordinates
        """
        neighborhoods = {
            'The Heights': (40.7485, -74.0453),
            'Newport': (40.7266, -74.0341),
            'Exchange Place': (40.7156, -74.0335),
            'Paulus Hook': (40.7147, -74.0406),
            'Hamilton Park': (40.7276, -74.0431),
            'Downtown': (40.7142, -74.0119),
            'Journal Square': (40.7334, -74.0679)
        }
        
        # Default to Jersey City if no match
        closest_neighborhood = 'Jersey City'
        min_distance = float('inf')
        
        for name, (nlat, nlon) in neighborhoods.items():
            # Simple distance calculation
            distance = ((lat - nlat)**2 + (lon - nlon)**2)**0.5
            if distance < min_distance:
                min_distance = distance
                closest_neighborhood = name
        
        return closest_neighborhood

    async def fetch_data(self, session: aiohttp.ClientSession, api_name: str) -> List[Dict]:
        headers = {
            'X-RapidAPI-Key': self.api_key,
            'X-RapidAPI-Host': self.apis[api_name]['host']
        }
        
        url = f"https://{self.apis[api_name]['host']}{self.apis[api_name]['endpoint']}"
        
        try:
            async with session.get(url, headers=headers, params=self.apis[api_name]['params']) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Successfully fetched data from {api_name}")
                    return self.parse_data(api_name, data)
                else:
                    response_text = await response.text()
                    logger.error(f"Error fetching data from {api_name}: {response.status}")
                    logger.error(f"Response: {response_text}")
                    return []
        except Exception as e:
            logger.error(f"Exception while fetching {api_name} data: {str(e)}")
            return []

    def parse_data(self, api_name: str, data: Dict) -> List[Dict]:
        parsed_properties = []
        
        if api_name == 'us-real-estate':
            try:
                if not data or 'data' not in data:
                    logger.error("No data found in API response")
                    return []

                home_search = data.get('data', {}).get('home_search', {})
                if not home_search:
                    logger.error("No home_search data found")
                    return []

                properties = home_search.get('results', [])
                if not properties:
                    logger.error("No results found in home_search data")
                    return []

                for prop in properties:
                    try:
                        description = prop.get('description', {})
                        location = prop.get('location', {}).get('address', {}) if prop.get('location') else {}
                        
                        # Skip properties that don't meet criteria
                        if not description or description.get('beds', 0) < 3:
                            continue
                        
                        price = prop.get('list_price', 0)
                        sqft = description.get('sqft', 0)
                        property_type = description.get('type', 'Unknown')
                        
                        # Get coordinates for neighborhood determination
                        coords = location.get('coordinate', {})
                        lat = coords.get('lat')
                        lon = coords.get('lon')
                        
                        # Determine neighborhood
                        neighborhood = self.determine_neighborhood(lat, lon) if lat and lon else 'Jersey City'
                        
                        # Estimate monthly costs
                        monthly_costs = self.estimate_monthly_costs(price, sqft, property_type)
                        
                        # Get listing URL
                        listing_url = f"https://www.realtor.com/realestateandhomes-detail/{prop.get('permalink', '')}" if prop.get('permalink') else ''
                        
                        parsed_properties.append({
                            'price': price,
                            'address': f"{location.get('line', '')} {location.get('city', '')} {location.get('state_code', '')}".strip(),
                            'bedrooms': description.get('beds', 0),
                            'bathrooms': description.get('baths', 0),
                            'sqft': sqft,
                            'property_type': property_type,
                            'thumbnail_url': prop.get('primary_photo', {}).get('href'),
                            'monthly_costs': monthly_costs,
                            'neighborhood': neighborhood,
                            'listing_url': listing_url
                        })
                    except Exception as e:
                        logger.error(f"Error parsing individual property: {str(e)}")
                        continue

                logger.info(f"Successfully parsed {len(parsed_properties)} properties")
            except Exception as e:
                logger.error(f"Error parsing properties: {str(e)}")
                return []

        elif api_name == 'zillow':
            return self.parse_zillow_data(data)
        
                
        return parsed_properties

    async def fetch_all_properties(self):
        async with aiohttp.ClientSession() as session:
            tasks = []
            
            for api_name in self.apis.keys():
                tasks.append(self.fetch_data(session, api_name))
            
            results = await asyncio.gather(*tasks)
            for result in results:
                self.properties.extend(result)

    def generate_html_report(self):
        """
        Generate a comprehensive HTML report of fetched properties
        """
        if not self.properties:
            logger.error("No properties to generate report")
            return False

        try:
            # Convert to DataFrame
            df = pd.DataFrame(self.properties)

            # Prepare HTML
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Holmdel Properties Report</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
        }}
        .property-card {{
            border: 1px solid #ddd;
            margin-bottom: 20px;
            padding: 15px;
            display: flex;
            align-items: start;
        }}
        .property-thumbnail {{
            width: 200px;
            height: 150px;
            object-fit: cover;
            margin-right: 15px;
        }}
        .property-details {{
            flex-grow: 1;
        }}
        .property-price {{
            font-size: 1.2em;
            font-weight: bold;
            color: #333;
        }}
        .property-link {{
            display: inline-block;
            background-color: #4CAF50;
            color: white;
            padding: 5px 10px;
            text-decoration: none;
            margin-top: 10px;
        }}
    </style>
</head>
<body>
    <h1>Jersey City Properties Report</h1>
    <p>Generated on: {current_time}</p>
"""

            # Generate property listings
            for _, row in df.iterrows():
                # Handle missing thumbnail
                thumbnail_html = (f'<img src="{row["thumbnail_url"]}" alt="Property Thumbnail" class="property-thumbnail">' 
                                  if row.get("thumbnail_url") else '')
                
                html_content += f"""
    <div class="property-card">
        {thumbnail_html}
        <div class="property-details">
            <h2>{row['address']}</h2>
            <p><strong>Neighborhood:</strong> {row['neighborhood']}</p>
            <p class="property-price">${row['price']:,.2f}</p>
            <p>{row['bedrooms']} beds | {row['bathrooms']} baths | {row['sqft']:,} sq ft</p>
            <p><strong>Monthly Non-Mortgage Costs:</strong> ${row['monthly_costs']['total_monthly_non_mortgage_costs']:,.2f}</p>
            {'<a href="' + row['listing_url'] + '" class="property-link" target="_blank">View Listing</a>' if row.get('listing_url') else ''}
        </div>
    </div>
"""

            html_content += """
</body>
</html>"""

            # Write to file
            output_file = os.path.expanduser('~/jersey_city_properties.html')
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(html_content)

            logger.info(f"Report generated at {output_file}")
            return True

        except Exception as e:
            logger.error(f"Error generating HTML report: {e}")
            import traceback
            traceback.print_exc()
            return False

async def main():
    """
    Main execution method
    """
    # Replace with your actual RapidAPI key
    # api_key = os.environ.get('RAPIDAPI_KEY', 'YOUR_RAPIDAPI_KEY')
    api_key = "9cd0949b08msha286b6987d46fe9p17feccjsn846e7f222f6f"
    
    aggregator = PropertyAggregator(api_key)
    logger.info("Starting to fetch property data...")
    
    await aggregator.fetch_all_properties()
    
    if aggregator.properties:
        report_success = aggregator.generate_html_report()
        logger.info(f"Found {len(aggregator.properties)} properties")
        if not report_success:
            logger.error("Failed to generate HTML report")
    else:
        logger.error("No properties were found. Please check your API key and try again.")

if __name__ == "__main__":
    asyncio.run(main())
