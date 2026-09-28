### stepit mitä projektissa pitää suorittaa:

1. hankitaan data sinne pilveen
2. Käsitellään data
    - jäljellä vaan tarvittavat attribuutit
    - data sellaiseen muotoon, että siitä voi tehdä tarvittavat laskut
        - Tietokantatyyppi? Mitä laskut vaatii
3. Analysoidaan data 
    - Tulokset johonkin muuhun dataformaattiin?
4. Joku mittapalikka mil mitataan kauan kestää
5.  Näytetään data frontendissä



## Kielet:

![kielet](image.png)

Ehdotus: 

Tehdään datan käsittely/datakomponentit GO:lla (tai javalla):
- Kuulemma helppo opii, hemo nopee, näyttää hyvält CV:ssä
- En tykkää pythonista plus se on hidas :(
- Scala/Java ihan jees, mut vaikeempi syntaksi ku GO:ssa, toisaalta ehkä tutumpi niin vois olla myös
- Myös C/C++/Rust mahdollisii mut nää on tosi vaikee oppii :D
- Fronttiin voi ja kannattaakin käyttää jotain ihan muuta


![alt text](image-2.png)


# Eli oikeesti tää systeemi toimis about näin:

1. Data producer  lukee csv:tä ja puskee reaaliaikasta tick dataa message brokeriin

2. Message broker on jaettu kahtteen osaan, josta toinen ottaa vastaan tick dataa ja toinen ottaa vastaan valmiiks laskettuja tuloksia

3. Stream analyzer lukee tick dataa ja laskee noita ikkunoita ja niistä laskee noi queryt. Lähettää valmiit tulokset message brokeriin

4. Web gateway, joka toimii "bäkkärinä" visualisaatioille. Eli websocket server joka streamaa livenä ikkunoita ja osto ja myynti ilmotuksia, Muistissa myös jonkunlainen historia, mistä saa haettua tietyillä symboleilla tai päivillä kontsaa (tietokanta?)

5. Web frontti

6. Joku Mittapalikka jossa on testejä ja muuta kivaa

Erikseen kantsii myös laskee ground truth noille tiedostoille, ainaki ekalle ja tokalle tms. Auttaa testaamisessa eikä ajaminen pitäis olla liian hidasta :D 


## lähteitä myöhästyneen datan käsittelyyn: 


 ### 1. The Definitive Paper on Event-Time, Watermarks, and Retractions

  │ Akidau, T., et al. (2015). The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing. Proceedings of the VLDB Endowment (PVLDB), 8(12), 1792–1803.
  │ https://doi.org/10.14778/2824032.2824076

  - What to cite it for: This is the landmark paper (by the Google Cloud Dataflow / Apache Beam team) that defined the modern principles of:
    - Event-time vs. processing-time windowing.
    - Watermarks and grace periods (allowed lateness).
    - Accumulating and Retracting mode (how systems issue correction events when late data arrives).


    @article{akidau2015dataflow,
      author    = {Tyler Akidau and Robert Bradshaw and Craig Chambers and Slava Chernyak and Rafael J. Fern{\'a}ndez-Moctezuma and Reuven Lax and Sam McVeety and Daniel Mills and Frances Perry and Eric Schmidt and Sam Whittle},
      title     = {The Dataflow Model: A Practical Approach to Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded, Out-of-Order Data Processing},
      journal   = {Proc. VLDB Endow.},
      volume    = {8},
      number    = {12},
      pages     = {1792--1803},
      year      = {2015},
      doi       = {10.14778/2824032.2824076}
    }
  ──────
  ### 2. The Original Low-Watermark & Stream Fault-Tolerance Paper

  │ Akidau, T., et al. (2013). MillWheel: Fault-Tolerant Stream Processing at Internet Scale. Proceedings of the VLDB Endowment (PVLDB), 6(11), 1033–1044. https://doi.org/10.14778/2536222.2536229

  - What to cite it for:
    - The original paper that invented low-watermarks to guarantee window completeness in distributed pipelines while keeping latency low. Cite this when justifying why your tumbling windows wait for a watermark before sealing.

    @article{akidau2013millwheel,
      author    = {Tyler Akidau and Alex Balikov and Kaya Bekiro{\u{g}}lu and Slava Chernyak and Josh Haberman and Reuven Lax and Sam McVeety and Daniel Mills and Paul Nordstrom and Frances Perry},
      title     = {MillWheel: Fault-Tolerant Stream Processing at Internet Scale},
      journal   = {Proc. VLDB Endow.},
      volume    = {6},
      number    = {11},
      pages     = {1033--1044},
      year      = {2013},
      doi       = {10.14778/2536222.2536229}
    }
  ──────
  ### 3. Event-Time Processing and Stateful Stream Systems

  │ Carbone, P., et al. (2015). Apache Flink™: Stream and Batch Processing in a Single Engine. IEEE Data Engineering Bulletin, 38(4), 28–38.

  - What to cite it for: 
    - Explains state management, event-time tumbling windows, and how distributed streaming engines handle out-of-order arrivals and state checkpointing without requiring heavy external databases.

    @article{carbone2015flink,
      author    = {Paris Carbone and Asterios Katsifodimos and Stephan Ewen and Volker Markl and Seif Haridi and Kostas Tzoumas},
      title     = {Apache Flink{\texttrademark}: Stream and Batch Processing in a Single Engine},
      journal   = {IEEE Data Eng. Bull.},
      volume    = {38},
      number    = {4},
      pages     = {28--38},
      year      = {2015}
    }
  ──────
  ### Bonus: The Official Dataset Paper (Must-Cite in Your Report)

  │ Frischbier, S., et al. (2022). The DEBS 2022 Grand Challenge: Detecting Trading Trends in Financial Market Data. In Proceedings of the 16th ACM International Conference on Distributed and Event-based Systems (DEBS '22), 164–170.
  │ https://doi.org/10.1145/3524860.3539643

  • What to cite it for: This is the exact paper by the creators of your dataset explaining the tick structure, attributes, and exchange characteristics.

    @inproceedings{frischbier2022debs,
      author    = {Sebastian Frischbier and Jawad Tahir and Christoph Doblander and Arne Hormann and Ruben Mayer and Hans-Arno Jacobsen},
      title     = {The DEBS 2022 Grand Challenge: Detecting Trading Trends in Financial Market Data},
      booktitle = {Proceedings of the 16th ACM International Conference on Distributed and Event-based Systems (DEBS '22)},
      pages     = {164--170},
      year      = {2022},
      doi       = {10.1145/3524860.3539643}
    }



![alt text](image-3.png)