package join_test

import (
	join "join/lib"
	"testing"

	"github.com/stretchr/testify/require"
)

var SumQuantityAndProfitById = []string{
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,6,3,9.5,28.5,2023-07-01 07:00:00",
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,6,1,9.5,9.5,2023-07-01 07:00:00",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,5,3,9.0,27.0,2023-07-01 07:00:02",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,1,1,6.0,6.0,2023-07-01 07:00:02",
	"85f86fef-fddb-4eef-9dc3-1444553e6108,7,3,9.0,27.0,2023-07-01 07:00:04",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,1,3,6.0,18.0,2023-07-01 07:00:21",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,6,1,9.5,9.5,2023-07-01 07:00:21",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,7,2,9.0,18.0,2023-07-01 07:00:21",
	"51e44c8e-4812-4a15-a9f9-9a46b62424d6,5,3,9.0,27.0,2023-07-01 07:00:33",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,2,3,7.0,21.0,2023-07-01 07:00:44",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,3,3,8.0,24.0,2023-07-01 07:00:44",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,6,3,9.5,28.5,2023-07-01 07:00:57",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,8,2,10.0,20.0,2023-07-01 07:00:57",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,6,3,9.5,28.5,2023-07-01 07:00:57",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,8,2,10.0,20.0,2023-07-01 07:01:01",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,5,3,9.0,27.0,2023-07-01 07:01:01",
	"bc9a368b-50d6-4f16-9505-edd8019c95ba,7,3,9.0,27.0,2023-07-01 07:01:20",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,7,3,9.0,27.0,2023-07-01 07:01:22",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,3,1,8.0,8.0,2023-07-01 07:01:22",
	"aaaa1111-bbef-493d-ae59-d4574132a8ae,7,3,9.0,27.0,2024-07-01 07:01:22",
	"bbbb2222-bbef-493d-ae59-d4574132a8ae,3,1,8.0,8.0,2025-07-01 07:01:22",
}

var Items = []string{
	"1,Espresso,coffee,6.0,False,,",
	"2,Americano,coffee,7.0,False,,",
	"3,Latte,coffee,8.0,False,,",
	"4,Cappuccino,coffee,8.0,False,,",
	"5,Flat White,coffee,9.0,False,,",
	"6,Mocha,coffee,9.5,False,,",
	"7,Hot Chocolate,non-coffee,9.0,False,,",
	"8,Matcha Latte,non-coffee,10.0,False,,",
}

var Stores = []string{
	"1,G Coffee @ USJ 89q,Jalan Dewan Bahasa 5/9,50998,USJ 89q,Kuala Lumpur,3.117134,101.615027",
	"2,G Coffee @ Kondominium Putra,Jln Yew 6X,63826,Kondominium Putra,Selangor Darul Ehsan,2.959571,101.51772",
	"3,G Coffee @ USJ 57W,Jalan Bukit Petaling 5/16C,62094,USJ 57W,Putrajaya,2.951038,101.663698",
	"4,G Coffee @ Kampung Changkat,Jln 6/6A,62941,Kampung Changkat,Putrajaya,2.914594,101.704486",
	"5,G Coffee @ Seksyen 21,Jalan Anson 4k,62595,Seksyen 21,Putrajaya,2.937599,101.698478",
	"6,G Coffee @ Alam Tun Hussein Onn,Jln Pasar Besar 63s,63518,Alam Tun Hussein Onn,Selangor Darul Ehsan,3.279175,101.784923",
	"7,G Coffee @ Damansara Saujana,Jln 8/74,65438,Damansara Saujana,Selangor Darul Ehsan,3.22081,101.58459",
	"8,G Coffee @ Bandar Seri Mulia,Jalan Wisma Putra,58621,Bandar Seri Mulia,Kuala Lumpur,3.140674,101.706562",
	"9,G Coffee @ PJS8,Jalan 7/3o,62418,PJS8,Putrajaya,2.952444,101.702623",
	"10,G Coffee @ Taman Damansara,Jln 2,67102,Taman Damansara,Selangor Darul Ehsan,3.497178,101.595271",
}

var ExpectedJoinedItems = []string{
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,Mocha,3,9.5,28.5,2023-07-01 07:00:00",
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,Mocha,1,9.5,9.5,2023-07-01 07:00:00",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,Flat White,3,9.0,27.0,2023-07-01 07:00:02",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,Espresso,1,6.0,6.0,2023-07-01 07:00:02",
	"85f86fef-fddb-4eef-9dc3-1444553e6108,Hot Chocolate,3,9.0,27.0,2023-07-01 07:00:04",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,Espresso,3,6.0,18.0,2023-07-01 07:00:21",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,Mocha,1,9.5,9.5,2023-07-01 07:00:21",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,Hot Chocolate,2,9.0,18.0,2023-07-01 07:00:21",
	"51e44c8e-4812-4a15-a9f9-9a46b62424d6,Flat White,3,9.0,27.0,2023-07-01 07:00:33",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,Americano,3,7.0,21.0,2023-07-01 07:00:44",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,Latte,3,8.0,24.0,2023-07-01 07:00:44",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,Mocha,3,9.5,28.5,2023-07-01 07:00:57",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,Matcha Latte,2,10.0,20.0,2023-07-01 07:00:57",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,Mocha,3,9.5,28.5,2023-07-01 07:00:57",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,Matcha Latte,2,10.0,20.0,2023-07-01 07:01:01",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,Flat White,3,9.0,27.0,2023-07-01 07:01:01",
	"bc9a368b-50d6-4f16-9505-edd8019c95ba,Hot Chocolate,3,9.0,27.0,2023-07-01 07:01:20",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,Hot Chocolate,3,9.0,27.0,2023-07-01 07:01:22",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,Latte,1,8.0,8.0,2023-07-01 07:01:22",
	"aaaa1111-bbef-493d-ae59-d4574132a8ae,Hot Chocolate,3,9.0,27.0,2024-07-01 07:01:22",
	"bbbb2222-bbef-493d-ae59-d4574132a8ae,Latte,1,8.0,8.0,2025-07-01 07:01:22",
}

func TestJoinerIsNotNil(t *testing.T) {
	joiner := join.NewJoiner()
	require.NotNil(t, joiner, "Expected NewJoiner to return a non-nil Join instance")
}

func TestJoinItemNameByIdEmpty(t *testing.T) {
	joiner := join.NewJoiner()
	joinedItems := joiner.JoinItemNameById([]string{}, SumQuantityAndProfitById)
	require.Empty(t, joinedItems, "Expected JoinItemNameById to return an empty slice")
}

func TestJoinItemById(t *testing.T) {
	joiner := join.NewJoiner()
	joinedItems := joiner.JoinItemNameById(Items, SumQuantityAndProfitById)
	require.Equal(t, ExpectedJoinedItems, joinedItems, "Joined items do not match expected results")
}
