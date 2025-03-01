import pandas as pd

data = pd.read_csv("../resources/clean/processed_social_media_entertainment_data.csv")
# defining the unique categories
gender_categories = data['Gender'].unique()
Age_groups = data['Age Group'].unique()
countries = ['United States', 'Canada', 'United Kingdom', 'Australia', 'India']
platforms = ['Facebook', 'Instagram', 'Twitter', 'TikTok', 'YouTube']

# reference tables
gender_ref = pd.DataFrame({'GenderID': range(1, len(gender_categories) + 1),
                           'GenderDescription': gender_categories})

age_group_ref = pd.DataFrame({'AgeGroupID': range(1, len(Age_groups) + 1),
                              'AgeGroupName': Age_groups})

country_ref = pd.DataFrame({'CountryID': range(1, len(countries) + 1),
                            'CountryName': countries,
                            'CountryCode': ['US', 'CA', 'GB', 'AU', 'IN']})

platform_ref = pd.DataFrame({'PlatformID': range(1, len(platforms) + 1),
                             'PlatformCode': range(1001, 1001 + len(platforms)),
                             'PlatformName': platforms})

# Map original data to reference IDs
data['GenderID'] = data['Gender'].map(gender_ref.set_index('GenderDescription')['GenderID'])
data['AgeGroupID'] = data['Age Group'].map(age_group_ref.set_index('AgeGroupName')['AgeGroupID'])
data['CountryID'] = data['Country'].map(country_ref.set_index('CountryName')['CountryID'])
data['PlatformID'] = data['Primary Platform'].map(platform_ref.set_index('PlatformName')['PlatformID'])

# saving the reference tables and update dataset
gender_ref.to_csv('resources/normalized/gender_ref.csv', index=False)
age_group_ref.to_csv('resources/normalized/age_group_ref.csv', index=False)
country_ref.to_csv('resources/normalized/country_ref.csv', index=False)
platform_ref.to_csv('resources/normalized/platform_ref.csv', index=False)
data.to_csv('resources/normalized/normalized_social_media_data.csv', index=False)

print("Normalization complete and reference tables saved.")
data.drop(columns=["Age", "Gender", "Country", "Primary Platform"], inplace=True)
data.to_csv('resources/normalized/normalized_social_media_data.csv', index=False)

print("Normalization complete and reference tables saved.")