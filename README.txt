CSC 369
Lab 3
Lana Huynh

Problem 1:
In CountryCount, I read in hostname_country.csv, and split the line up into hostname and country. I then made this my
key and value. I then split each line of the access log on whitespace and got the hostname. In the reduce portion,
I summed up the counts of the countries. In ReverseKeyValue, I reversed the key and value pairs in order to sort in
descending order.

Problem 2:
In CountryURLCount, I did the same process in problem 1, but I made the key hostname and url. In GroupByCountryURLCount,
I made a comparator to first compare the country and then counts in order to sort properly.

Problem 3:
In URLCountryCount, my setup was the same. In the map part, I made the url my key and the country my value. In the
reduce part, I iterated through the countries to put them in a HashSet, ensuring that there are only unique values. I
then put the HashSet into a list so that I am able to alphabetically sort the countries. I turned into this into a
string and wrote it out.
