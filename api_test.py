# """Just little tests
# """

# # Little API test
# import dam4ml

# # from dam4ml import DAM4MLClient, Dataset, formatter, attribute

# # Connect MNIST-Test
# client = dam4ml.Client(
#     project_slug="mnist-test",
#     username="numericube",
#     token="dK_fm2Ijg3pa09gSfnU8_QWXE81yLkOgHNLVxyiQvy8",
#     api_url="http://localhost:8000/api/v1/",
# )

# # Just a quick test to check if we're okay, and filter things.
# # print(dataset.api.projects("mnist-test").get())
# x_y_formatter = dam4ml.formatters.TupleFormatter(
#     dam4ml.attributes.ImageIO(), dam4ml.attributes.TagRegex(r"[0-9]", flatten=True)
# )
# test_dataset = client.dataset(tag_slug="test", formatter=x_y_formatter)
# val_dataset = client.dataset(
#     tag_slug="validation",
#     formatter=(
#         dam4ml.attributes.ImageIO(),
#         dam4ml.attributes.TagRegex(r"[0-9]", flatten=True),
#     ),
#     batch_size=100000,
# )

# # [OPTIONAL] Preload dataset
# val_dataset.load()

# # [OPTIONAL] Check/Print dataset size
# print(len(test_dataset))
# print(test_dataset[4])

# # Open each file just to check if they're okay
# for asset in test_dataset:
#     print(asset[0].shape, asset[1])

# # Another way to see the problem is as a batched result
# for x_val, y_val in val_dataset:
#     assert len(x_val) == len(y_val)

# # Let's upload a sample image (by filename)
# client.upload(
#     "./requirements.txt",
#     tags=("3", "9", "abc"),
# )
