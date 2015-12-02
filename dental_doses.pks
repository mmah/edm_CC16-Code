 
create or replace function dental_doses (input_cost number,input_quantity varchar2, 
input_service_id number, input_patient_id number) return number as

input_vi_species_id varchar2(10);
input_product_info varchar2(100);
input_package_quantity varchar2(100);
input_package_size varchar2(100);
input_package_volume varchar2(100);
return_package_count number;

begin

select case when vi_species_id = '3' then 'canine' 
            when vi_species_id = '7' then 'feline' 
            else 'other' end
into input_vi_species_id 
from cooked.patient 
where patient_id = input_patient_id;

select product_info,package_quantity,package_size,package_volume 
into input_product_info,input_package_quantity,input_package_size,input_package_volume 
from cooked.service_general 
where service_general_id = input_service_id;

if INPUT_PACKAGE_QUANTITY is not null then
  if INPUT_PACKAGE_QUANTITY = input_quantity then
    return to_number(REGEXP_REPLACE(input_package_quantity, '[^0-9]+', '')); -- for example when we see a quantity of 30 and package size of 30 we're pretty confident that it's 30 not 900
  end if;
  if normalization.isnumeric(INPUT_PACKAGE_QUANTITY) = 1 then 
    return INPUT_PACKAGE_QUANTITY*input_quantity; -- if we have package_quantity from SG then multiply by transaction quantity to get doses
  else
    return input_quantity;
  end if;
end if;

if input_package_size is not null and input_package_volume is not null  and REGEXP_REPLACE(input_package_volume, '[^0-9]+', '') is not null then
  select package_count into return_package_count 
  from dental_matrix
  where product = input_product_info
  and REGEXP_REPLACE(package_volume, '[^0-9]+', '') = REGEXP_REPLACE(input_package_volume, '[^0-9]+', '')
  and treat_size = input_package_size;
  return return_package_count*input_quantity;
end if;


--- add code for one record in matrix for a specific product,package volume
if REGEXP_REPLACE(input_package_volume, '[^0-9]+', '') is not null then 
  select m.package_count into return_package_count 
  from dental_matrix m inner join 
  (select --count(*),
  product
  ,REGEXP_REPLACE(package_volume, '[^0-9]+', '') package_volume
  from dental_matrix
  where package_volume = REGEXP_REPLACE(input_package_volume, '[^0-9]+', '')
  and product = input_product_info
  group by product
  ,REGEXP_REPLACE(package_volume, '[^0-9]+', '')
  having count(*) = 1) a on a.product = m.product and REGEXP_REPLACE(a.package_volume, '[^0-9]+', '') = REGEXP_REPLACE(m.package_volume, '[^0-9]+', '');
  return return_package_count*input_quantity;
end if;

--return input_package_size;

--- add code for one record in matrix for a specific product,treat size
if input_package_size is not null then 
  select m.package_count into return_package_count 
  from dental_matrix m inner join 
  (select --count(*),
  product
  ,treat_size
  from dental_matrix
  where treat_size = input_package_size
  and product = input_product_info
  group by product
  ,treat_size
  having count(*) = 1) a on a.product = m.product and a.treat_size = m.treat_size
  ;

  return return_package_count*input_quantity;
end if;



return input_quantity;
  exception
  when no_data_found then
    return input_quantity;
  
end; 
