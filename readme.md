OPIS:
Data Vault 2.0 w oparciu o architekture Medallion zaimplementowałem przy użyciu technologii Apache Hudi + Spark. 
Same dane są dość proste, skorzystałem ze zbioru AdventureWorksDW2022 z laboratoriów, ograniczając się do 3 wymiarów i 1 faktu.
Hurtownia uruchamiana jest jako pipeline - po wykonaniu skrypt pobierane dane z lokalnej bazy danych, przeprowadza procesowania zgodnie z zadaną architekturą, na wyjściu w warstwie Gold jest plik CSV z myślą o analizie w Power BI.
Z uwagi na to że rozwiązanie ma być warsztatową demonstracją pozwoliłem sobie na wiele uproszczeń; m.in ograniczyłem liczbę wierszy pobieranych z tabel by przyspieszyć czas działania i procesowania danych.
#TODO dodać plik konfiguracyjny?

IMPLEMENTACJA:
Z uwagi na to że narzędzia Apache... nie lubią sie z Windowsem konfiguracja środowiska była bardzo uciążliwa, i z tej uwagi pipiline jest "zkonteneryzowany" w instacji Dockera - obraz jest budowany i wykonywany, odpowiadając jednemu przetworzeniu danych. Dane są zapisywane jako "append", dołączane do istniejących.

W Dockerfile zhardcode-owane są scieżki do repozytoriów Maven'a do odpowiednich wersji bibliotek zapewniając bezproblemowe uruchomienie; wszystko powinno zostać automatycznie pobrane.

INSTRUKCJA URUCHOMIENIA:
- ARCHITEKTURA ROZWIĄZANIA ZAKŁADA DZIAŁANIE SERWERA - TRZEBA SKONFIGUROWAĆ PRZED URUCHOMIENIEM:
    - w wersji załączonej na repo jest konfiguracja odpowiadająca mojej lokalnej, czyli SQLExpress, do bazy danych o nazwie AdventureWorksDW2022.
    - !! serwer musi mieć zezwoloną komunikację TCP IP na porcie 1433.
    - !! serwer musi uwierzytelniać po stronie SQL Server.
- URUCHOMIENIE:
    - ZBUDUJ OBRAZ:
    docker build spark-hudi .

    - URUCHOM PIPELINE:
    docker run --rm -v [ścieżka do katalogu lakehouse do mount instancji] spark-hudi
    przykładowo:
    docker run --rm -v  C:/spark-hudi/data:/data spark-hudi