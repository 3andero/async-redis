#[macro_export]
macro_rules! BytesToString {
    ($bytes: expr) => {
        String::from_utf8($bytes.to_vec()).map_err(|e| Box::new(e))?
    };
    ($bytes: expr, $err_type: expr) => {
        String::from_utf8($bytes.to_vec()).map_err(|e| $err_type(Box::new(e)))?
    };
}
