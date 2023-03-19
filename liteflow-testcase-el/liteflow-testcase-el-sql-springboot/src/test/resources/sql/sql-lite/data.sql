DELETE FROM EL_TABLE;

INSERT INTO EL_TABLE (APPLICATION_NAME,CHAIN_NAME,EL_DATA) values ('demo','chain1','THEN(a, b, c);');
INSERT INTO EL_TABLE (APPLICATION_NAME,CHAIN_NAME,EL_DATA) values ('demo','chain2','THEN(a, b, c);');
INSERT INTO EL_TABLE (APPLICATION_NAME,CHAIN_NAME,EL_DATA) values ('demo','chain3','IF(x0, THEN(a, b));');
INSERT INTO EL_TABLE (APPLICATION_NAME,CHAIN_NAME,EL_DATA) values ('demo','<chain3>','IF(x0, THEN(a, b));');
INSERT INTO EL_TABLE (APPLICATION_NAME,CHAIN_NAME,EL_DATA) values ('demo','chain4','IF(x2, IF(x0, THEN(a, b)));');

DELETE FROM SCRIPT_NODE_TABLE;

INSERT INTO SCRIPT_NODE_TABLE (APPLICATION_NAME,SCRIPT_NODE_ID,SCRIPT_NODE_NAME,SCRIPT_NODE_TYPE,SCRIPT_NODE_DATA,SCRIPT_LANGUAGE) values ('demo','x0','if 脚本','if_script','return true','groovy');
INSERT INTO SCRIPT_NODE_TABLE (APPLICATION_NAME,SCRIPT_NODE_ID,SCRIPT_NODE_NAME,SCRIPT_NODE_TYPE,SCRIPT_NODE_DATA,SCRIPT_LANGUAGE) values ('demo','x1','if 脚本','if_script','return false','groovy');

INSERT INTO SCRIPT_NODE_TABLE (APPLICATION_NAME,SCRIPT_NODE_ID,SCRIPT_NODE_NAME,SCRIPT_NODE_TYPE,SCRIPT_NODE_DATA,SCRIPT_LANGUAGE) values ('demo','x2','python脚本','if_script','return true','js');
