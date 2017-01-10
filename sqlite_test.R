library("RSQLite")

setwd("c:/programs/r")

#************FIRST PART: CREATE DATABASE************
#start new SQLite database (file)
db <- dbConnect(SQLite(),dbname="test.sqlite")

#start new tables inside the new database (deleting table first if it existed already)
dbSendQuery(conn=db,"DROP TABLE IF EXISTS observation")
dbSendQuery(conn=db,"DROP TABLE IF EXISTS species")
dbSendQuery(conn=db,"DROP TABLE IF EXISTS location")
dbSendQuery(conn=db,"DROP TABLE IF EXISTS date")
dbSendQuery(conn=db,"CREATE TABLE observation(ID INTEGER PRIMARY KEY UNIQUE, row VARCHAR(128) UNIQUE, 
            date_ID INTEGER, location_ID INTEGER, species_ID, number INTEGER)")
dbSendQuery(conn=db,"CREATE TABLE species(ID INTEGER PRIMARY KEY UNIQUE, 
            species VARCHAR(128) UNIQUE, substrate VARCHAR(128))")
dbSendQuery(conn=db,"CREATE TABLE location(ID INTEGER PRIMARY KEY UNIQUE, location VARCHAR(128) UNIQUE)")
dbSendQuery(conn=db,"CREATE TABLE date(ID INTEGER PRIMARY KEY UNIQUE, date VARCHAR(128) UNIQUE)")

#import the csv's into dataframes
samples <- read.csv("samples.csv",stringsAsFactors = F)
species <- read.csv("species.csv",stringsAsFactors = F)

#fill the sql-database by looping through the r-dataframes
#first the r-dataframe with the species, this goes only into the sql-table species
nspecies <- dim(species)[1]
for (i in 1:nspecies) {
  name1 <- toString(species[i,][1])
  name2 <- toString(species[i,][4])
  names <- list(name1,name2)
  dbSendQuery(conn=db,"INSERT OR IGNORE INTO species (species,substrate) 
              VALUES (?,?)",names)
}
#then the r-dataframe with the observations, this goes into three sql-tables:
#into date, location and observation
nobservations <- dim(samples)[1]
for (j in 1:nobservations) {
  #table date: gets date
  datej <- toString(samples[j,][1])
  dbSendQuery(conn=db,"INSERT OR IGNORE INTO date (date) VALUES (?)",datej)
  #table location: gets location
  locationj <- toString(samples[j,][2])
  dbSendQuery(conn=db,"INSERT OR IGNORE INTO location (location) VALUES (?)",
              locationj)
  #table observation gets: row, date_ID, location_ID, species_ID and number
  numberj <- toString(samples[j,][4])
  speciesj <- toString(samples[j,][3])
  dateID <- dbGetQuery(conn=db,"SELECT ID FROM date WHERE date = ?",datej)
  dateID <- toString(dateID)
  locationID <- dbGetQuery(conn=db,"SELECT ID FROM location WHERE location = ?",locationj)
  locationID <- toString(locationID)
  speciesID <- dbGetQuery(conn=db,"SELECT ID FROM species WHERE species = ?",speciesj)
  speciesID <- toString(speciesID)
  rowj <- paste(dateID,locationID,speciesID,numberj,sep="")
  names <- list (rowj,dateID,locationID,speciesID,numberj)
  dbSendQuery(conn=db,"INSERT OR IGNORE INTO observation (row,date_ID,location_ID,
              species_ID,number) VALUES (?,?,?,?,?)",names)
}

#************SECOND PART: QUERY DATABASE************
#select observations made on date 1
datex <- 'May2015'
datexID <- dbGetQuery(conn=db,"SELECT ID FROM date WHERE date=?",datex)
datexID <- toString(datexID)
dbGetQuery(conn=db,"SELECT observation.number,date.date
           FROM date JOIN observation
           ON observation.date_ID = date.ID 
           WHERE observation.date_ID = ?",datexID)

#find all mya for all dates and all locations, excluding zeros
speciesx <- 'mya'
speciesxID <- dbGetQuery(conn=db,"SELECT ID FROM species WHERE species=?",speciesx)
speciesxID <- toString(speciesxID)
myadata_no0 <- dbGetQuery(conn=db,"SELECT date.date,location.location,species.species,observation.number
           FROM observation JOIN location JOIN species JOIN date
           ON observation.date_ID = date.ID 
           AND observation.location_ID = location.ID 
           AND observation.species_ID = species.ID
           WHERE observation.species_ID = ?",speciesxID)
myadata_no0

#find all mya for all dates and all locations, including zeros
speciesx <- 'mya'
myadata_with0 <- dbGetQuery(conn=db,"SELECT date.date,location.location,species.species,SUM(observation.number)
          FROM species JOIN date LEFT JOIN location LEFT JOIN observation
          ON location.ID = observation.location_ID
          AND date.ID = observation.date_ID
          AND species.ID = observation.species_ID
          WHERE species = ? GROUP BY date",speciesx)
myadata_with0

#find the sum for all species per date
myadata_grouped <- dbGetQuery(conn=db,"SELECT date.date,species.species,SUM(observation.number)
          FROM species JOIN date LEFT JOIN observation
          ON date.ID = observation.date_ID
          AND species.ID = observation.species_ID
          GROUP BY species,date")
myadata_grouped

#how can you do a query on a query result? This is done by nesting:
results1 <- dbGetQuery(conn=db,"SELECT * FROM observation WHERE date_ID=1")
results1
results2 <- dbGetQuery(conn=db,"SELECT * FROM
          (SELECT * FROM observation WHERE date_ID=1)
          WHERE species_ID = '2'
          ")
results2