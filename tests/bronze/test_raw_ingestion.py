import pytest
import requests

def test_api_response_status():
    url = "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=5"
    response = requests.get(url)
    assert response.status_code == 200

def test_api_response_not_empty():
    url = "https://api.openbrewerydb.org/v1/breweries?page=1&per_page=5"
    data = requests.get(url).json()
    assert len(data) > 0
