import { useDispatch, useSelector } from "react-redux";
import { fetchCampers } from "../../redux/productsOps.js";
import { setPage } from "../../redux/productsSlice.js";
import styles from "./LoadMoreBtn.module.css";
import "react-toastify/dist/ReactToastify.css";
import { toast, ToastContainer } from "react-toastify";

const LoadMoreBtn = () => {
  const dispatch = useDispatch();
  const { currentPage, totalPages, limit } = useSelector(
    (state) => state.campers,
  );

  const handleLoadMore = () => {
    const nextPage = currentPage + 1;

    if (totalPages && nextPage >= totalPages) {
      toast.info("Sorry, no more results...");
      return;
    }

    dispatch(setPage(nextPage));
    dispatch(fetchCampers({ page: nextPage, limit }));
  };

  const isLastPage = totalPages && currentPage >= totalPages;

  return (
    <div>
      {!isLastPage && (
        <button className={styles.container} onClick={handleLoadMore}>
          Load More
        </button>
      )}
      <ToastContainer position="top-center" autoClose={3000} />
    </div>
  );
};

export default LoadMoreBtn;
