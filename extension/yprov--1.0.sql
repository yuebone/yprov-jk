 CREATE OR REPLACE FUNCTION yprov_open()
 RETURNS integer
 AS
 '/home/bon/proj/ypg/prov/build/libyprov.so','yprov_open'
 LANGUAGE C;

 CREATE OR REPLACE FUNCTION yprov_close()
 RETURNS integer
 AS
 '/home/bon/proj/ypg/prov/build/libyprov.so','yprov_close'
 LANGUAGE C;
