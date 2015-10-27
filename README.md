# SimpleIRC-Kafka
Tugas IF4031 Pengembangan Aplikasi Terdistribusi
"Aplikasi chat sederhana dengan Apache Kafka"

Hayyu' Luthfi Hanifah (13512080)
Choirunnisa Fatima (13512084)

##Petunjuk Instalasi/Building
Pada root direktori, jalankan perintah `mvn clean install package` 

##Petunjuk Menjalankan Program
Untuk menjalankan aplikasi, jalankan perintah `mvn exec:java`

##Daftar Test yang Dilakukan
Test dilakukan dengan menjalankan 1 Server dan 3 Client. Client 1 (nick: hayyu) tergabung dalam channel lab dan channel1. Client 2 (nick: icha) tergabung dalam channel lab. Client 3 (nick: user3) tergabung dalam channel channel1. 

Berikut adalah test yang dilakukan:

1. Memberikan perintah `/NICK`
	Kasus normal: `/NICK hayyu`
	Output		: `Online as hayyu`

	Kasus nick kosong	: `/NICK`
	Output				: `Online as userX` (dengan X adalah nomor user)

2. Memberikan perintah `/JOIN`
	Kasus normal: `/JOIN lab`
	Output		: `Join channel lab`

	Kasus nama grup kosong  : `/JOIN`
	Output					: `Join channel channelX` (dengan X adalah nomor channel)

3. Mengirim pesan
	Kasus normal:
		Client 1: `@lab halo semua`
		Client 2 (output): `[lab] (hayyu) halo semua`
		Client 3 (output): - 

	Kasus nama channel kosong
		Client 1: `halo semua`
		Client 2 (output): `[lab] (hayyu) halo semua`
		Client 3 (output): `[channel1] (hayyu) halo semua`

4. Keluar dari channel
		Client 1: `/LEAVE lab`
		Client 1 (output): `Leaving channel lab`
		Client 2: `/LEAVE channel1`
		Client 2 (output): `You're not registered as member of channel1`
		Client 3: `/LEAVE lala`
		Client 3 (output): `Channel not found`
		Client 1: `/LEAVE`
		Client 1 (output): `Channel name is empty`

6. Memberikan perintah `/EXIT`
	Kasus normal:
		Client 1: `/EXIT`
		Client 1 (output): `Going offline...`

7. Mengirim pesan ke channel yang hanya terdiri dari 1 user (user lain sudah leave/exit)
	Client 2: `@lab test lab`
	Client 2 (output): `[lab] (icha) test lab`
	Client 1 (output): - (Client 1 sudah leave channel lab)


