create database vendas;


-- create table for produtos
create table produtos(id SERIAL primary key,
codigo_barra varchar(40) not null,
nome varchar(40) not null,
descricao varchar(40) not null,
embalagem varchar(40) not null,
preco DECIMAL not null);

-- create table for vendas
create table item_vendas ( id SERIAL primary key,
produto_id integer not null,
quantidade DECIMAL not null,
valor_total DECIMAL not null,
descricao varchar(40) not null,
foreign key (produto_id) references produtos(id) );

-- Insert produtos
insert
	into
	produtos(codigo_barra,
	nome,
	descricao,
	embalagem,
	preco)
values ('123456789',
'Coca-cola 2lt',
'refrigerante coca-cola 2 litros',
'LT',
5.99);

insert
	into
	produtos(codigo_barra,
	nome,
	descricao,
	embalagem,
	preco)
values ('123456788',
'Coca-cola 1,5lt',
'refrigerante coca-cola 1,5 litros',
'LT',
4.99);

insert
	into
	produtos(codigo_barra,
	nome,
	descricao,
	embalagem,
	preco)
values ('123456780',
'Coca-cola 600ml',
'refrigerante coca-cola 600 ml',
'LT',
5.99);

-- insert vendas
insert
	into
	item_vendas(produto_id,
	quantidade,
	valor_total,
	descricao)
values (1,
50,
299.5,
'Compra para distribuicao');

insert
	into
	item_vendas(produto_id,
	quantidade,
	valor_total,
	descricao)
values (1,
50,
299.5,
'Compra para distribuicao');

insert
	into
	item_vendas(produto_id,
	quantidade,
	valor_total,
	descricao)
values (1,
50,
299.5,
'Compra para distribuicao');

