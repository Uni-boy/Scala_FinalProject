# CSYE7200 - Scala_FinalProject

## Game Recommendation System

### Group Members:

- Chen Ye - 002951477 [@yechen0704](https://github.com/yechen0704)
- Xinzhuo Liu - 002197134 [@ZhongLBuL](https://github.com/ZhongLBuL)
- Jingru Xiang - 001586653 [@RubyXiang](https://github.com/RubyXiang)

### Outlier:
![Alt text](https://github.com/Uni-boy/Scala_FinalProject/blob/ChenYe/presentation/GRecCons.png)

### Data Source:
  - User game association table:
https://www.kaggle.com/code/danieloehm/steam-game-recommendations/data
  - Game rating association table:
https://www.kaggle.com/datasets/nikdavis/steam-store-games

### Methodology :
  - Scala
  - Spark
  - Collaborative Filtering
  - Swagger UI
  - Play Framework
  - MongoDB

### Dependencies:
  - Scala v2.12
  - Spark v3.1.3
  - sbt v1.6.2
  - MongoDB v4.4, Mongodb Compass(uri: "mongodb://localhost:27017/testdb")

(Our project only needs to use sbt to manage external packages and run all parts)

### Instruction:

Firstly, you need import *Scala_FinalProject*, *collaborativeFiltering*, *dataPreprocess*, *play*, *recommendation* directories as modules when you start the project to run unit test, and then:
1. Data Preprocess: '**cd dataPreprocess**', in 'dataPreprocess' directory, use '**sbt run**' to start.
When finished, 'game.collection', 'train.collection', 'validation.collection', and 'test.collection' can be seen in Mongodb Compass.
2. Train Model: '**cd collaborativeFiltering**', in 'collaborativeFiltering' directory, use '**sbt -v -J-Xmx2048m run**' to train and validate data from Mongodb.
 When finished, the directory 'myModel.model' will appear in 'collaborativeFiltering' directory, and 'validPred', which stores the prediction values, is created in MongoDB.
3. Static Prediction: '**cd recommendation**', in 'recommendation' directory, use '**sbt run**' to create prediction collection 'testPred' for test collection.
4. Start System & Dynamic Prediction: '**cd play**', in 'play' directory, use '**sbt -v -J-Xmx2048m run**' to start project. Then, to input 'http://localhost:9000/' on web page to use our system.
