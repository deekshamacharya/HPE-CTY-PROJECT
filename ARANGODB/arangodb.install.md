# ARANGODB INSTALLATON

## First add the repository key to apt like this:

curl -OL https://download.arangodb.com/arangodb311/DEBIAN/Release.key<br>
sudo apt-key add - < Release.key

## Use apt-get to install arangodb:

echo 'deb https://download.arangodb.com/arangodb311/DEBIAN/ /' | sudo tee /etc/apt/sources.list.d/arangodb.list<br>
sudo apt-get install apt-transport-https<br>
sudo apt-get update<br>
sudo apt-get install arangodb3=3.11.8-1

## To install the debug symbols package (not required by default)

sudo apt-get install arangodb3-dbg=3.11.8-1
