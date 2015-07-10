# Delta-Pie

Delta Pie takes an aribtrary SQL statement, a list of saved columns and a list of degrees.

It then generates a csv with the % change from one rwo to the next.

If your query returns say..

5
10
20

and you asked for a degree of 1, it would give you two rows showing that there were 100% gains.
If you asked for a degree of 2, it would give you one extra row with a increase of 167%, as it averages the last 2 rows and calculates the difference in %.
