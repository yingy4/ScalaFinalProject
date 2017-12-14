# ScalaFinalProject :: Airline Trends: Prices and Passengers

The travel industry is a multi-billion dollar industry, with quite rapidly fluctating prices and uncertainity. This can be quite frustrating from a consumer point of view, especially during peak seasons. Presently, the airline is consolidated with just a few major key players that allow them to fluctuate the prices at will.

We plan to analyze the past trends in the fickle industry and try to provide future predictions of how the prices will vary over days and weeks. We also plan to analyze the travelling patterns of people, especially during the holiday season and check how the industry also float their prices in response to the overwhelming number of people who travel.

In addition to the above analysis, we plan to provide a web UI which can act like a dashboard of aggregated results and provide recommnedations to the customer on when and which flight to book.

There are two Apps. The PlaySVNTravels app is a play application which does the real time flight processing and Spark Analysis application does the historical data analysis.

To run PlaySVNTravels app you will need a Kafka and Zookeeper servers running. For the Spark analysis application you will need the data set and HBase.


The PlaySVNTravels might throw an error while compile or build which relates to the "view.html.index", you can ignore that. That is just a bug with the IDE. To not get it again you can run the "sbt clean" followed by the "sbt compile" commands.

Steps to run PlaySVNTravels :
  1. Start kafka server and zookeeper servers. If you don't have it you can install it through homebrew via the command "brew      install kafka" or install it from the apache site (https://kafka.apache.org/quickstart).
  
      If you installed via homebrew you can use the command "zookeeper-server-start /path/to/your/zookeeper.properties &             kafka-server-start /path/to/your/server.properties".
  2. After starting kafka and zookeeper, start the play app which will start the netty server which play internally uses.
  3. Once all the servers are up, use a websocket client of your choice using a browser of your choice, connect to the              websocket and make a request.
  
  
  Steps to run Spark Analysis App:
  1. Install Hbase
  2. Run the project with the dataset.
  
  The screenshots of the unit test cases are in the folder named Unit tests.
  The screenshots of the UI and results are included in the Flight Data Analysis PPT.
