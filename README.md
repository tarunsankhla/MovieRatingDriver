<h1>Analyzing most-rated movies by their average rating using Secondary Sorting in Hadoop </h1>
Objective: 
The goal is to determine movies that received the most ratings and ensure they are grouped by movieId and sorted by their average rating using Secondary Sorting in Hadoop. This is achieved by using a custom sorting mechanism at the framework level, eliminating the need for manual sorting in the reducer.
</br>
<h2>Use Case:</h2>
<p>
  # Use Cases for Secondary Sorting in Hadoop

## 1. Streaming Platform Analytics

**Problem:**  
A streaming platform (e.g., Netflix) wants to display the most popular movies and ensure they are sorted by their average ratings.

**Solution:**  
Use secondary sorting to compute the highest-rated movies that also received the most ratings.

**Outcome:**  
- Results can be displayed directly to users as **"Top-Rated Movies"** or **"Most Popular Movies."**

---

## 2. Academic Institutions

**Problem:**  
An academic institution wants to rank professors or courses by their number of student reviews and average ratings.

**Solution:**  
Apply secondary sorting to group professors/courses by `professorId` or `courseId` and sort by the average ratings.

**Outcome:**  
- Results can be used for **institutional analytics** or **public ranking** of professors or courses.

---

## Core Concepts

1. **Grouping and Sorting**  
   - The custom `MovieRating` class ensures movies are grouped by their `movieId`.
   - Within each group, the `averageRating` values are sorted in **descending** order.

2. **Custom WritableComparable**  
   - The `compareTo` method in `MovieRating` ensures **descending** order for ratings and **ascending** order for `movieId` for grouping.

3. **Sorting at Reducer Input**  
   - Hadoop automatically sorts data by the `compareTo` logic **before** it reaches the reducer.
   - This eliminates the need for an explicit sorting step in the reducer.

---

## Key Differences

|                        | **Secondary Sorting Output**                             | **Normal Sorting Output**                        |
|------------------------|-----------------------------------------------------------|--------------------------------------------------|
| **Output Format**      | Movies grouped by `movieId` and **sorted by** `averageRating` within each group. | Movies grouped by `movieId` but **not** sorted by `averageRating`. |
| **Optimization**       | Optimized for direct usage when retrieving highest-rated movies. | Requires additional steps (e.g., custom logic) to sort results externally. |

---

## Verify Outputs

- **Secondary Sorting Output**  
  - Grouped **and** sorted by `averageRating`.
- **Normal Sorting Output**  
  - Grouped but **unsorted** by `averageRating`.

</p>
<br/>
<pre>
 scp /mnt/c/Users/tarun/Documents/GitHub/INFO_7250_Big_data/final_project/MovieRatingDriver/target/MovieRatingDriver-1.0-SNAPSHOT.jar  hdoop@tarunsankhla:~/MovieRatingDriver-1.0-SNAPSHOT.jar
hadoop jar ~/MovieRatingDriver-1.0-SNAPSHOT.jar
 edu.neu.csye6220.movieratingdriver.MovieRatingDriver /final_project/ratings.csv /user/hdoop/movie_rating_sec_sort
hadoop jar ~/MovieRatingDriver-1.0-SNAPSHOT.jar edu.neu.csye6220.movieratingdriver.MovieRatingDriver /final_project/ratings.csv /user/hdoop/movie_rating_sec_sort_1

hdfs dfs -cat /user/hdoop/movie_rating_sec_sort/part-r-00000 | head
 hdfs dfs -cat /user/hdoop/movie_rating_sec_sort/_SUCCESS | head 
hdfs dfs -ls /user/hdoop/movie_rating_sec_sort
hdfs dfs -rm -r /user/hdoop/movie_rating_sec_sort
hadoop fs -mkdir /user/hdoop/movie_rating_sec_sort

</pre>
 
![image](https://github.com/user-attachments/assets/026f5bd1-32ce-4997-97b6-b86a1c4fa49c)
<br/>
