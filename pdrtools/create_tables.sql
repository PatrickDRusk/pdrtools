/*
# mysqlimport -u root -p  --local --fields-terminated-by=, ruskie ContractPriceData.csv

create table ContractMetadata (
contract VARCHAR(100),
security VARCHAR(100),
expiration DATE,
inception DATE
);

create table ContractPriceData (
log_s VARCHAR(100),
open FLOAT,
close FLOAT,
volume FLOAT,
high FLOAT,
low FLOAT,
open_interest FLOAT,
security VARCHAR(100),
contract VARCHAR(100)
);

create table Security (
  security_id int not null auto_increment,
  symbol VARCHAR(30) not null,
  PRIMARY KEY (security_id),
  UNIQUE (symbol)
);



drop table Contract;
create table Contract (
  contract_id int not null auto_increment,
  symbol VARCHAR(30) not null,
  security_id int not null,
  inception DATE,
  expiration DATE,
  PRIMARY KEY (contract_id),
  UNIQUE (symbol)
);

drop table PriceData;
create table PriceData (
  price_id int not null auto_increment,
  contract_id int not null,
  log_s date not null,
  open float,
  close float,
  volume float,
  high float,
  low float,
  open_interest float,
  PRIMARY KEY (price_id),
  UNIQUE (contract_id, log_s)
);
*/

drop view Prices;
CREATE VIEW Prices AS
select s.symbol as security, c.symbol as contract,
       pd.*
from PriceData pd
join Contract c
on c.contract_id = pd.contract_id
join Security s
on s.security_id = c.security_id
;


