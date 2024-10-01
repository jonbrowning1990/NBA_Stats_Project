
SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema database_year_name
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema database_year_name
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `database_year_name` DEFAULT CHARACTER SET utf8mb3 ;
USE `database_year_name` ;

-- -----------------------------------------------------
-- Table `database_year_name`.`games`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_year_name`.`games` (
  `gameID` INT NOT NULL AUTO_INCREMENT,
  `Home` VARCHAR(45) NOT NULL,
  `Away` VARCHAR(45) NOT NULL,
  `HomeScore` INT NOT NULL,
  `AwayScore` INT NOT NULL,
  `Date` DATE NOT NULL,
  `Time` TIME NOT NULL,
  `Attendence` INT NOT NULL,
  `url` VARCHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`gameID`))
ENGINE = InnoDB
AUTO_INCREMENT = 1
DEFAULT CHARACTER SET = utf8mb3;


-- -----------------------------------------------------
-- Table `database_year_name`.`players`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_year_name`.`players` (
  `PlayerID` INT NOT NULL AUTO_INCREMENT,
  `TeamID` INT NOT NULL,
  `Number` INT NULL DEFAULT NULL,
  `Name` VARCHAR(45) NOT NULL,
  `Position` VARCHAR(45) NULL DEFAULT NULL,
  `Height` VARCHAR(45) NULL DEFAULT NULL,
  `Weight` INT NULL DEFAULT NULL,
  `DOB` DATE NULL DEFAULT NULL,
  PRIMARY KEY (`PlayerID`, `TeamID`))
ENGINE = InnoDB
AUTO_INCREMENT = 1
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


-- -----------------------------------------------------
-- Table `database_year_name`.`teams`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_year_name`.`teams` (
  `TeamID` INT NOT NULL AUTO_INCREMENT,
  `Name` VARCHAR(45) NOT NULL,
  `City` VARCHAR(45) NOT NULL,
  `Mascot` VARCHAR(100) NULL DEFAULT NULL,
  PRIMARY KEY (`TeamID`))
ENGINE = InnoDB
AUTO_INCREMENT = 1
DEFAULT CHARACTER SET = utf8mb3;


-- -----------------------------------------------------
-- Table `database_year_name`.`gamestats`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `database_year_name`.`gamestats` (
  `GameID` INT NOT NULL,
  `TeamID` INT NOT NULL,
  `PlayerID` INT NOT NULL,
  `Player` VARCHAR(100) NOT NULL,
  `Starter` TINYINT NOT NULL,
  `MP` VARCHAR(10) NOT NULL,
  `FG` INT NOT NULL,
  `FGA` INT NOT NULL,
  `3FG` INT NOT NULL,
  `3FGA` INT NOT NULL,
  `FT` INT NOT NULL,
  `FTA` INT NOT NULL,
  `ORB` INT NOT NULL,
  `DRB` INT NOT NULL,
  `TRB` INT NOT NULL,
  `AST` INT NOT NULL,
  `STL` INT NOT NULL,
  `BLK` INT NOT NULL,
  `TOV` INT NOT NULL,
  `PF` INT NOT NULL,
  `PTS` INT NOT NULL,
  `Plus_Minus` INT NOT NULL,
  `Home_Away` VARCHAR(45) NOT NULL,
  PRIMARY KEY (`GameID`, `PlayerID`, `TeamID`),
  INDEX `PlayerID` (`PlayerID` ASC),
  INDEX `gamestats_ibfk_2_idx` (`TeamID` ASC),
  CONSTRAINT `gamestats_ibfk_1`
    FOREIGN KEY (`GameID`)
    REFERENCES `database_year_name`.`games` (`gameID`),
  CONSTRAINT `gamestats_ibfk_2`
    FOREIGN KEY (`PlayerID`)
    REFERENCES `database_year_name`.`players` (`PlayerID`),
  CONSTRAINT `gamestats_ibfk_3`
    FOREIGN KEY (`TeamID`)
    REFERENCES `database_year_name`.`teams` (`TeamID`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
COLLATE = utf8mb4_0900_ai_ci;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
