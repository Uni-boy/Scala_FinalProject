# CSYE7200 - Scala_FinalProject
## Game Recommendation System
### Group Members:
- Chen Ye - 002951477​
- Xinzhuo Liu - 002197134​
- Jingru Xiang - 001586653 

### Outlier:
![Alt text]()

### Data Source:
  - User game association table
https://www.kaggle.com/code/danieloehm/steam-game-recommendations/data
  - Game tag association table

### Instruction:
1. In 'dataPreprocess' directory, use 'sbt run' to start date preprocess
2. In 'collaborativeFiltering' directory, use 'sbt -v -J-Xmx2048m run' to train and validate data from Mongodb
3. In 'recommendation' directory, use 'sbt run' to create prediction collection for test collection
2. In 'play' directory, use 'sbt run' to start project
