import styles from "./FavoriteBtn.module.css";
import { useDispatch, useSelector } from "react-redux";
import { selectFavorites, toggleFavorite } from "../redux/favoritesSlice.js";

const FavoriteBtn = ({ camperId }) => {
  const dispatch = useDispatch();
  const favorites = useSelector(selectFavorites);

  const isFavorite = favorites.includes(camperId);

  const handleToggleFavorite = () => {
    dispatch(toggleFavorite(camperId));
  };

  return (
    <svg
      onClick={handleToggleFavorite}
      className={styles.favorite}
      width={26}
      height={24}
    >
      <use
        href={`./icons.svg#${
          isFavorite ? "icon-favorite-pressed" : "icon-favorite-default"
        }`}
      />
    </svg>
  );
};

export default FavoriteBtn;
