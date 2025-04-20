# Diary

I started doing this in another project and it's a good way for me to keep
track of things. So here starts the diary.

## TODO

* Insert schema stuff into the LogicalPlan creation so that `*` works etc. (Might not be needed for *)
* Create a `Visitor` trait for `LogicalPlan` that I can implement for things like printing etc
* Autoincrement will stop working if you close and reopen the database
* Logical plan implementation for filtering, sorting and think about joins...
* Proper DATE type

## Resources

* Look at [datafusion](https://docs.rs/datafusion/latest/datafusion/) because I'm lost on mapping from sql to relational algebra stuff
* Also read [Translation SQL into Relational Algebra](https://cs.ulb.ac.be/public/_media/teaching/infoh417/sql2alg_eng.pdf)
* [Introduction to Database Systems CSE444](https://courses.cs.washington.edu/courses/cse444/09sp/lectures/lecture18.pdf) lists logical operators
* [CMU15-799 Query Optimization](https://15799.courses.cs.cmu.edu/spring2025/schedule.html)
* [Query Engines Push vs Pull](https://justinjaffray.com/query-engines-push-vs.-pull/)
* [How Query Engines Work](https://howqueryengineswork.com/00-acknowledgments.html) - holy hell!

## 2025-04-20

* Preserve the column ordering from INSERT statements

## 2025-04-19

* Made the selection node of logical plan, some restructuring
* Shift around example so it can generate a query without a schema for now but we need schemas 
* Need to implement something for joins and then we're roughly there?

## 2025-04-12

* Started implementing Expressions as part of the WHERE predicate handling
* Implemented `DROP TABLE`

## 2025-04-11

* Implemented logical plan generation for limit, selecting from tables and columns.

## 2025-04-10

* Reading the [Datafusion paper](http://andrew.nerdnetworks.org/other/SIGMOD-2024-lamb.pdf) good top level overview
* Reading [Query Planning and Optimisation](https://15445.courses.cs.cmu.edu/spring2024/notes/15-optimization1.pdf) more detailed
lecture notes
* Thoughts: just store parsed sql for select, then require storage engine to make logical plan to check it makes sense? Or make logical plan
and then validate against storage engine? Maybe it's worth having a schema I can grab for each table, and just making it available for parsing?


## 2025-01-25

* What was I doing, I'm so lost.
* Okay found datafusion again, I'm back on track
