from mrjob.job import MRJob
import re
from countryinfo import CountryInfo
from geosky import geo_plug
import locationtagger

WORD_RE= re.compile(r"[^\n]+")

country = CountryInfo('Australia')

def country_locations(country):
    """Retrieve all states and their cities in Australia"""
    locations = []
    for capital_city in country.provinces():
        locations.extend(re.compile(r'"[\w ]+"').findall(geo_plug.all_State_CityNames(capital_city)))
    return locations

def remove_quotes_from_items(lst):
    """Removes quotes from items, i.e. 'Australian Capital Territory' instead of '"Australian Capital Territory"'"""
    adjusted = []
    for item in lst:
        item = item.replace('"','')
        item = item.lower()
        adjusted.append(item)
    return adjusted

def extract_location(txt):
    """Extract Australian location names from the text"""
    # Use locationtagger to extract state and city names from the text
    place_entity = locationtagger.find_locations(text = txt)
    if (len(place_entity.cities) > 0) and ("Australia" in place_entity.country_cities.keys()):
        return str(place_entity.country_cities["Australia"][0])
    else:
        if (len(place_entity.regions) > 0):
            for state in place_entity.regions:
                if state in country.provinces():
                    return "unknown city - " +  str(state)
        else:
            # Brute-force solution: If locationtagger fails to extract location names from the text, 
            # use the Australian state and city list to check whether any word in the text belongs in the list.
            adjusted_locations = remove_quotes_from_items(country_locations(country))
            for location in adjusted_locations:
                # Any item of the city and state list identified in the text but not in the state list would be the city
                if (location in txt.lower()) and (location.title() not in country.provinces()):
                    return location.title()
                # Any item of the city and state list identified in the text and in the state list would be the state 
                elif (location in txt.lower()) and (location.title() in country.provinces()):
                    return "unknown city - " +  location.title()
            # if no item from the state and city list is identified, check whether "australia" or "aus" is contained in the text
            if ("australia" in txt.lower()) or ("aus" in txt.lower()):
                    return "unknown city - unknown state"

class MRAustralianCityFrequencyCount(MRJob):

    def mapper(self,_,line):
        for word in WORD_RE.findall(line):
            yield extract_location(word), 1

    def reducer(self,word,counts):
        yield word, sum(counts)

if __name__ == '__main__':
    MRAustralianCityFrequencyCount.run()