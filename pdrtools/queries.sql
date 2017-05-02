insert into Security (symbol) select distinct security from ContractMetadata;

insert into Contract (symbol, security_id, inception, expiration)
select m.contract, s.security_id, m.inception, m.expiration
from ContractMetadata m
join Security s
on m.security = s.symbol
order by s.security_id, m.expiration
;

insert into PriceData (contract_id, log_s, open, close, volume, high, low, open_interest)
select c.contract_id, p.log_s, p.open, p.close, p.volume, p.high, p.low, p.open_interest
from ContractPriceData p
join Contract c
on p.contract = c.symbol
order by contract_id, log_s
;

select log_s, close
from PriceData
where (contract_id = 1 and log_s between '1988-06-23' and '1988-08-05')
or (contract_id = 2 and log_s between '1988-08-06' and '1988-09-05')
or (contract_id = 3 and log_s between '1988-09-05' and '1988-10-05')
or (contract_id = 4 and log_s between '1988-10-06' and '1988-11-06')
order by log_s
;

select log_s, close
from Prices
where (contract = 'COU88_COMDTY' and log_s between '1988-06-23' and '1988-08-05')
or (contract = 'COV88_COMDTY' and log_s between '1988-08-06' and '1988-09-05')
or (contract = 'COX88_COMDTY' and log_s between '1988-09-05' and '1988-10-05')
or (contract = 'COZ88_COMDTY' and log_s between '1988-10-06' and '1988-11-06')
order by log_s
;

select * from Contract;
