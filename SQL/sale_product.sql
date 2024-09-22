WITH ranked_products AS (
    SELECT 
        product_class.product_class_name,
        ROW_NUMBER() OVER ( -- Assign a rank to each product within its class
            PARTITION BY product_class.product_class_name -- Group by product class name
            ORDER BY 
                (product.retail_price * sales_trans.quantity) DESC, -- Order by sales value (descending)
                sales_trans.quantity ASC -- Then by quantity (ascending)
        ) AS rank,
        product.productname,
        (product.retail_price * sales_trans.quantity) AS sales_value,
        sales_trans.quantity
    FROM `project.dataset.product` AS product
    LEFT JOIN `project.dataset.Sales Transaction` AS sales_trans ON product.product_id = sales_trans.product_id
    LEFT JOIN `project.dataset.Product Class` AS product_class ON product._class_id = product_class.product_class_id
)

SELECT 
    product_class_name, 
    rank, 
    productname AS product_name, 
    sales_value 
FROM ranked_products
WHERE rank <= 2
ORDER BY product_class_name, rank;