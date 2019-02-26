# LitematPlusPlus



## Annexe
Aiming to test our system in extreme situations, i.e. manipulating a large number of assertions, 
we created some datasets based on LUBM that only contain the assertions of the ***lubm:subOrganisationOf*** transitive property.
Some parameters can be set while using this generator. An example is offered below to run the generator with these parameters.

<pre><code>#!/bin/bash
scala creatChain.scala --num-univ 1000 --max-depart-num 25 --min-depart-num 20 --max-depth 5 --min-depth 2 --max-branches-num 20 --min-branches-num 10 --path /Volumes/UNTITLED/xchain/
</code></pre>

**--num-univ** means the number of universities appears in the dataset, **--max-depart-num** and **--min-depart-num** limit the number of department appearing in each university. **--max-depth** and **--num-univ**

By using this generator, we made several datasets which are mainly in form of transitive chain and of transitive trees. 

In the LUBM ontology, a department must be a sub-organization of a university and a group must be a sub-organization of a department. On the contrary, in our datasets, we consider that there exists a hierarchy relation among the groups in each department belonging to a university and we ignore the relations among universities and departments.
