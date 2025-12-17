import styles from "./Features.module.css";
import Loader from "../Loader/Loader.jsx";
import ErrorMessage from "../ErrorMessage/ErrorMessage.jsx";
import {useSelector} from "react-redux";
import {
    selectCamper,
    selectError,
    selectLoading,
} from "../../redux/productsSlice.js";
import EquipmentList from "../EquipmentList/EquipmentList.jsx";
// import BookForm from "../BookForm/BookForm.jsx";

const Features = () => {
    const camper = useSelector(selectCamper);
    const loading = useSelector(selectLoading);
    const errorMessage = useSelector(selectError);

    return (
        <div className={styles.container}>
            <div className={styles.additionalInfoContainer}>
                <EquipmentList camper={camper}/>
                <div className={styles.vehicleDetails}>
                    <h4 className={styles.vehicleDetailsTitle}>Vehicle details</h4>
                    <ul className={styles.vehicleDetailsList}>
                        <li className={styles.vehicleDetailsItem}>
                            <span className={styles.vehicleDetailsItemName}>Form</span>
                            <div>{camper.form}</div>
                        </li>
                        <li className={styles.vehicleDetailsItem}>
                            <span className={styles.vehicleDetailsItemName}>Length</span>
                            <div>{camper.length}</div>
                        </li>
                        <li className={styles.vehicleDetailsItem}>
                            <span className={styles.vehicleDetailsItemName}>Width</span>
                            <div>{camper.width}</div>
                        </li>
                        <li className={styles.vehicleDetailsItem}>
                            <span className={styles.vehicleDetailsItemName}>Height</span>
                            <div>{camper.height}</div>
                        </li>
                        <li className={styles.vehicleDetailsItem}>
                            <span className={styles.vehicleDetailsItemName}>Tank</span>
                            <div>{camper.tank}</div>
                        </li>
                        <li className={styles.vehicleDetailsItem}>
                            <span className={styles.vehicleDetailsItemName}>Consumption</span>
                            <div>{camper.consumption}</div>
                        </li>
                    </ul>
                </div>
            </div>
            <div className={styles.formContainer}>
                {/*<BookForm/>*/}
            </div>
            {loading && <Loader/>}
            {errorMessage && <ErrorMessage errorMessage={errorMessage}/>}
        </div>
    );
};

export default Features;
