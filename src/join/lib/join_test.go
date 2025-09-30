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

var Transactions = []string{
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,4,5,,,38.0,0.0,38.0,2023-07-01 07:00:00",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,7,1,,,33.0,0.0,33.0,2023-07-01 07:00:02",
	"85f86fef-fddb-4eef-9dc3-1444553e6108,1,5,,,27.0,0.0,27.0,2023-07-01 07:00:04",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,5,2,,,45.5,0.0,45.5,2023-07-01 07:00:21",
	"aaaa1111-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2024-07-01 07:02:21",
	"bbbb1111-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2024-07-01 07:02:21",
	"cccc2222-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2024-07-01 07:02:21",
	"51e44c8e-4812-4a15-a9f9-9a46b62424d6,8,5,,,27.0,0.0,27.0,2023-07-01 07:00:33",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,8,3,,,45.0,0.0,45.0,2023-07-01 07:00:44",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,3,1,,,77.0,0.0,77.0,2023-07-01 07:00:57",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,5,5,,,47.0,0.0,47.0,2023-07-01 07:01:01",
	"bc9a368b-50d6-4f16-9505-edd8019c95ba,5,5,,,27.0,0.0,27.0,2023-07-01 07:01:20",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,2,2,,,43.0,0.0,43.0,2023-07-01 07:01:22",
	"85508712-16bc-474f-af7c-23966680e76e,8,1,,,42.0,0.0,42.0,2023-07-01 07:01:23",
	"d921113a-a18f-496e-9283-d62182f9322d,5,3,,,54.5,0.0,54.5,2023-07-01 07:01:34",
	"725e0e23-02e7-43a9-9162-3089ed93ef9c,6,1,,,54.0,0.0,54.0,2023-07-01 07:01:37",
	"ed939e86-8545-4632-991a-1e523d2c36a8,7,2,,,69.5,0.0,69.5,2023-07-01 07:01:42",
	"6108f270-73ef-4fc1-8e6d-107f3d8082ef,4,1,,,43.0,0.0,43.0,2023-07-01 07:01:43",
	"eb89be5f-db4f-4e0d-9196-8bebaac57f33,6,1,,,36.0,0.0,36.0,2023-07-01 07:01:52",
	"48968d91-dd5a-47f2-8646-42f8b587932f,3,1,,,30.0,0.0,30.0,2023-07-01 07:01:54",
	"825e124c-dcc0-4e07-82fb-1b4634a49808,2,2,,,19.0,0.0,19.0,2023-07-01 07:02:15",
	"dddd3333-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"eeee4444-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"qqqq5555-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"rrrr6666-edbf-456c-bbd5-31aa56dc96c9,8,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"zzzz6666-edbf-456c-bbd5-31aa56dc96c9,8,1,,,120.1,0.0,14.0,2025-07-01 07:02:21",
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

var TransactionsJoined = []string{
	"2ae6d188-76c2-4095-b861-ab97d3cd9312,G Coffee @ Kampung Changkat,5,,,38.0,0.0,38.0,2023-07-01 07:00:00",
	"7d0a474d-62f4-442a-96b6-a5df2bda8832,G Coffee @ Damansara Saujana,1,,,33.0,0.0,33.0,2023-07-01 07:00:02",
	"85f86fef-fddb-4eef-9dc3-1444553e6108,G Coffee @ USJ 89q,5,,,27.0,0.0,27.0,2023-07-01 07:00:04",
	"4c41d179-f809-4d5a-a5d7-acb25ae1fe98,G Coffee @ Seksyen 21,2,,,45.5,0.0,45.5,2023-07-01 07:00:21",
	"aaaa1111-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2024-07-01 07:02:21",
	"bbbb1111-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2024-07-01 07:02:21",
	"cccc2222-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2024-07-01 07:02:21",
	"51e44c8e-4812-4a15-a9f9-9a46b62424d6,G Coffee @ Bandar Seri Mulia,5,,,27.0,0.0,27.0,2023-07-01 07:00:33",
	"d449cf8f-e6d5-4b09-a02e-693c7889dee8,G Coffee @ Bandar Seri Mulia,3,,,45.0,0.0,45.0,2023-07-01 07:00:44",
	"6b00c575-ec6e-4070-82d2-26d66b017b8b,G Coffee @ USJ 57W,1,,,77.0,0.0,77.0,2023-07-01 07:00:57",
	"54fa4304-5131-4382-a8dc-f30cb18155b7,G Coffee @ Seksyen 21,5,,,47.0,0.0,47.0,2023-07-01 07:01:01",
	"bc9a368b-50d6-4f16-9505-edd8019c95ba,G Coffee @ Seksyen 21,5,,,27.0,0.0,27.0,2023-07-01 07:01:20",
	"fe97c4a3-bbef-493d-ae59-d4574132a8ae,G Coffee @ Kondominium Putra,2,,,43.0,0.0,43.0,2023-07-01 07:01:22",
	"85508712-16bc-474f-af7c-23966680e76e,G Coffee @ Bandar Seri Mulia,1,,,42.0,0.0,42.0,2023-07-01 07:01:23",
	"d921113a-a18f-496e-9283-d62182f9322d,G Coffee @ Seksyen 21,3,,,54.5,0.0,54.5,2023-07-01 07:01:34",
	"725e0e23-02e7-43a9-9162-3089ed93ef9c,G Coffee @ Alam Tun Hussein Onn,1,,,54.0,0.0,54.0,2023-07-01 07:01:37",
	"ed939e86-8545-4632-991a-1e523d2c36a8,G Coffee @ Damansara Saujana,2,,,69.5,0.0,69.5,2023-07-01 07:01:42", // Note: Store id 7, name is "G Coffee @ Damansara Saujana"
	"6108f270-73ef-4fc1-8e6d-107f3d8082ef,G Coffee @ Kampung Changkat,1,,,43.0,0.0,43.0,2023-07-01 07:01:43",
	"eb89be5f-db4f-4e0d-9196-8bebaac57f33,G Coffee @ Alam Tun Hussein Onn,1,,,36.0,0.0,36.0,2023-07-01 07:01:52",
	"48968d91-dd5a-47f2-8646-42f8b587932f,G Coffee @ USJ 57W,1,,,30.0,0.0,30.0,2023-07-01 07:01:54",
	"825e124c-dcc0-4e07-82fb-1b4634a49808,G Coffee @ Kondominium Putra,2,,,19.0,0.0,19.0,2023-07-01 07:02:15",
	"dddd3333-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"eeee4444-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"qqqq5555-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"rrrr6666-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,14.0,0.0,14.0,2025-07-01 07:02:21",
	"zzzz6666-edbf-456c-bbd5-31aa56dc96c9,G Coffee @ Bandar Seri Mulia,1,,,120.1,0.0,14.0,2025-07-01 07:02:21",
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

func TestJoinTransactionsAndStores(t *testing.T) {
	joiner := join.NewJoiner()
	joinedStores := joiner.JoinItemNameById(Stores, Transactions)
	require.Equal(t, TransactionsJoined, joinedStores, "Expected JoinStoreNameById to return an empty slice")
}
