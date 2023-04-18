USE WAREHOUSE DEFAULT_WH;

--drop table if exists DEEP_PURPLE.public.ps_map_incrementality_weights;

create table DEEP_PURPLE.public.ps_map_incrementality_weights (
country varchar
, os varchar
, platform varchar
, weight float
);

INSERT INTO DEEP_PURPLE.public.ps_map_incrementality_weights (
    country
    , os
    , platform
    , weight
) 
VALUES 
('US','iOS','Meta','1.1'),
('US','iOS','GAC','1.05'),
('US','iOS','Google Search','0.9'),
('US','Android','Meta','1.1'),
('US','Android','GAC','1.05'),
('US','Android','Google Search','0.9'),
('US','Web','Meta','1.1'),
('US','Web','GAC','1.05'),
('US','Web','Google Search','0.9'),
('France','iOS','Meta','1.1'),
('France','iOS','GAC','1.2'),
('France','iOS','Google Search','0.8'),
('France','Android','Meta','1.1'),
('France','Android','GAC','1.2'),
('France','Android','Google Search','0.8'),
('France','Web','Meta','1.1'),
('France','Web','GAC','1.2'),
('France','Web','Google Search','0.8'),
('US','iOS','Meta','1.1'),
('US','iOS','GAC','1.05'),
('US','iOS','Google Search','0.9'),
('US','Android','Meta','1.1'),
('US','Android','GAC','1.05'),
('US','Android','Google Search','0.9'),
('US','Web','Meta','1.1'),
('US','Web','GAC','1.05'),
('US','Web','Google Search','0.9'),
('France','iOS','Meta','1.1'),
('France','iOS','GAC','1.2'),
('France','iOS','Google Search','0.8'),
('France','Android','Meta','1.1'),
('France','Android','GAC','1.2'),
('France','Android','Google Search','0.8'),
('France','Web','Meta','1.1'),
('France','Web','GAC','1.2'),
('France','Web','Google Search','0.8');
